const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const { Client } = require('ssh2');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);

// ===== HTTP 永不超时 =====
server.timeout = 0;
server.keepAliveTimeout = 0;
server.headersTimeout = 0;
server.requestTimeout = 0;

// 增加 Socket.IO 最大消息大小限制 - 无限制
const io = socketIo(server, {
    cors: { origin: "*" },
    transports: ['websocket'],
    pingInterval: 10000,
    pingTimeout: 60000,
    allowEIO3: true,
    maxHttpBufferSize: 1e10,
    maxPayload: 1e10
});

app.use(express.static(path.join(__dirname, 'public')));
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

// 增加 express 文件大小限制
app.use(express.json({ limit: '10gb' }));
app.use(express.urlencoded({ limit: '10gb', extended: true }));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// 配置文件存储路径
const CONFIG_FILE = path.join(__dirname, 'config.json');

// 读取配置
function loadConfig() {
    try {
        if (fs.existsSync(CONFIG_FILE)) {
            const data = fs.readFileSync(CONFIG_FILE, 'utf8');
            return JSON.parse(data);
        }
    } catch (err) {
        console.error('读取配置文件失败:', err);
    }
    return { groups: [], servers: [], nextGroupId: 1, nextServerId: 1 };
}

// 保存配置
function saveConfig(config) {
    try {
        fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 2), 'utf8');
        return true;
    } catch (err) {
        console.error('保存配置文件失败:', err);
        return false;
    }
}

// 获取配置 API
app.get('/api/config', (req, res) => {
    const config = loadConfig();
    res.json(config);
});

// 保存配置 API
app.post('/api/config', express.json(), (req, res) => {
    const config = req.body;
    if (saveConfig(config)) {
        res.json({ success: true, message: '配置保存成功' });
    } else {
        res.status(500).json({ success: false, message: '配置保存失败' });
    }
});

// 导入配置 API
app.post('/api/config/import', express.json(), (req, res) => {
    const importedConfig = req.body;
    
    if (!importedConfig.groups || !importedConfig.servers) {
        res.status(400).json({ success: false, message: '配置文件格式错误' });
        return;
    }
    
    const newGroups = [];
    const newServers = [];
    let newGroupId = 1;
    let newServerId = 1;
    const groupIdMap = new Map();
    
    for (const group of importedConfig.groups) {
        const newGroup = {
            id: newGroupId++,
            name: group.name,
            collapsed: group.collapsed || false
        };
        newGroups.push(newGroup);
        groupIdMap.set(group.id, newGroup.id);
    }
    
    if (newGroups.length === 0) {
        const defaultGroup = { id: newGroupId++, name: '默认分组', collapsed: false };
        newGroups.push(defaultGroup);
        groupIdMap.set(1, defaultGroup.id);
    }
    
    for (const server of importedConfig.servers) {
        const newServer = {
            id: newServerId++,
            groupId: groupIdMap.get(server.groupId) || newGroups[0].id,
            name: server.name,
            host: server.host,
            port: server.port || 22,
            username: server.username,
            password: server.password || ''
        };
        newServers.push(newServer);
    }
    
    const newConfig = {
        groups: newGroups,
        servers: newServers,
        nextGroupId: newGroupId,
        nextServerId: newServerId
    };
    
    if (saveConfig(newConfig)) {
        res.json({ success: true, message: '配置导入成功', config: newConfig });
    } else {
        res.status(500).json({ success: false, message: '配置导入失败' });
    }
});

// 导出配置 API
app.get('/api/config/export', (req, res) => {
    const config = loadConfig();
    const exportConfig = {
        groups: config.groups,
        servers: config.servers.map(s => ({
            id: s.id,
            groupId: s.groupId,
            name: s.name,
            host: s.host,
            port: s.port,
            username: s.username,
            password: s.password
        })),
        exportTime: new Date().toISOString(),
        version: '1.0'
    };
    res.json(exportConfig);
});

// 存储所有SSH连接会话 - 每个 socket 独立存储
// 使用 Map: key 为 socketId + sessionId 的组合
const sshConnections = new Map();

io.on('connection', (socket) => {
    console.log(`[${new Date().toLocaleString()}] Socket连接: ${socket.id}`);

    let destroyTimer = null;

    // =========================
    // 创建独立的 SSH 连接
    // =========================
    function createSSH(sessionId, config) {
        if (!config) return;

        // 生成唯一的连接ID（socketId + sessionId）
        const connectionId = `${socket.id}_${sessionId}`;

        // 关闭现有连接
        if (sshConnections.has(connectionId)) {
            const existing = sshConnections.get(connectionId);
            if (existing.sshClient) {
                try { existing.sshClient.end(); } catch (e) {}
            }
            sshConnections.delete(connectionId);
        }

        const sshClient = new Client();
        let sshStream = null;
        let isConnected = false;
        let sftp = null;
        let reconnectTimer = null;
        let uploadStream = null;

        const connectionData = {
            sshClient,
            sshStream: null,
            isConnected: false,
            config,
            sftp: null,
            reconnectTimer: null,
            uploadStream: null,
            connectionId: connectionId
        };

        sshClient.on('tcp connection', (tcp) => {
            tcp.setKeepAlive(true, 10000);
            tcp.setNoDelay(true);
        });

        sshClient.on('ready', () => {
            console.log(`[${new Date().toLocaleString()}] SSH已连接: ${connectionId}`);
            isConnected = true;
            connectionData.isConnected = true;

            socket.emit('session-ready', { sessionId });
            socket.emit('session-data', {
                sessionId,
                data: `\r\n\x1b[32m✓ 已连接到 ${config.username}@${config.host}:${config.port}\x1b[0m\r\n`
            });

            sshClient.sftp((err, sftpClient) => {
                if (err) {
                    console.log(`SFTP初始化失败 ${connectionId}:`, err.message);
                    socket.emit('sftp-error', { sessionId, error: err.message });
                } else {
                    sftp = sftpClient;
                    connectionData.sftp = sftp;
                    socket.emit('sftp-ready', { sessionId });
                    
                    sftp.on('close', () => {
                        console.log(`SFTP关闭: ${connectionId}`);
                        connectionData.sftp = null;
                    });
                }
            });

            sshClient.shell({
                term: 'xterm-256color',
                cols: 120,
                rows: 30
            }, (err, stream) => {
                if (err) {
                    socket.emit('session-data', {
                        sessionId,
                        data: `\r\n\x1b[31mShell错误: ${err.message}\x1b[0m\r\n`
                    });
                    return;
                }

                sshStream = stream;
                connectionData.sshStream = stream;

                stream.on('data', (data) => {
                    if (socket.connected) {
                        socket.emit('session-data', {
                            sessionId,
                            data: data.toString('utf-8')
                        });
                    }
                });

                stream.on('close', () => {
                    console.log(`Shell关闭: ${connectionId}`);
                    connectionData.sshStream = null;
                    if (isConnected) {
                        reconnectSSH(connectionId, sessionId);
                    }
                });

                stream.stderr.on('data', (data) => {
                    if (socket.connected) {
                        socket.emit('session-data', {
                            sessionId,
                            data: data.toString()
                        });
                    }
                });
            });
        });

        sshClient.on('error', (err) => {
            console.log(`SSH错误 ${connectionId}:`, err.message);
            isConnected = false;
            connectionData.isConnected = false;
            socket.emit('session-data', {
                sessionId,
                data: `\r\n\x1b[31mSSH错误: ${err.message}\x1b[0m\r\n`
            });
            reconnectSSH(connectionId, sessionId);
        });

        sshClient.on('close', () => {
            console.log(`SSH关闭: ${connectionId}`);
            isConnected = false;
            connectionData.isConnected = false;
            if (connectionData.sftp) {
                try { connectionData.sftp.end(); } catch(e) {}
                connectionData.sftp = null;
            }
            if (connectionData.uploadStream) {
                try { connectionData.uploadStream.end(); } catch(e) {}
                connectionData.uploadStream = null;
            }
            reconnectSSH(connectionId, sessionId);
        });

        function reconnectSSH(connId, sid) {
            if (connectionData.reconnectTimer) return;
            
            console.log(`准备重连SSH: ${connId}`);
            connectionData.reconnectTimer = setTimeout(() => {
                connectionData.reconnectTimer = null;
                if (connectionData.config) {
                    createSSH(sid, connectionData.config);
                }
            }, 3000);
        }

        sshClient.connect({
            host: config.host,
            port: config.port || 22,
            username: config.username,
            password: config.password,
            readyTimeout: 60000,
            keepaliveInterval: 10000,
            keepaliveCountMax: 999999,
            algorithms: {
                serverHostKey: ['ssh-rsa', 'ecdsa-sha2-nistp256', 'ssh-ed25519']
            }
        });

        sshConnections.set(connectionId, connectionData);
    }

    function closeSession(sessionId) {
        const connectionId = `${socket.id}_${sessionId}`;
        const connection = sshConnections.get(connectionId);
        if (connection) {
            if (connection.reconnectTimer) {
                clearTimeout(connection.reconnectTimer);
            }
            if (connection.sshStream) {
                try { connection.sshStream.close(); } catch (e) {}
            }
            if (connection.sftp) {
                try { connection.sftp.end(); } catch (e) {}
            }
            if (connection.uploadStream) {
                try { connection.uploadStream.end(); } catch (e) {}
            }
            if (connection.sshClient) {
                try { connection.sshClient.end(); } catch (e) {}
            }
            sshConnections.delete(connectionId);
        }
    }

    // =========================
    // 文件上传
    // =========================
    socket.on('sftp-upload-start', ({ sessionId, remotePath, fileName, fileSize }) => {
        const connectionId = `${socket.id}_${sessionId}`;
        const connection = sshConnections.get(connectionId);
        if (!connection || !connection.sftp) {
            socket.emit('sftp-upload-error', { sessionId, error: 'SFTP未就绪' });
            return;
        }
        
        let targetPath;
        if (remotePath === '/' || remotePath === '') {
            targetPath = `/${fileName}`;
        } else {
            targetPath = `${remotePath}/${fileName}`;
        }
        
        console.log(`开始上传: ${targetPath}, 大小: ${(fileSize / 1024 / 1024).toFixed(2)} MB`);
        
        const writeStream = connection.sftp.createWriteStream(targetPath);
        
        writeStream.on('error', (err) => {
            console.error(`上传错误 ${targetPath}:`, err.message);
            socket.emit('sftp-upload-error', { sessionId, error: err.message });
            connection.uploadStream = null;
        });
        
        writeStream.on('finish', () => {
            console.log(`上传完成: ${targetPath}`);
            socket.emit('sftp-upload-complete', { sessionId });
            connection.uploadStream = null;
        });
        
        connection.uploadStream = writeStream;
    });
    
    socket.on('sftp-upload-data', ({ sessionId, data }) => {
        const connectionId = `${socket.id}_${sessionId}`;
        const connection = sshConnections.get(connectionId);
        if (!connection || !connection.uploadStream) return;
        
        try {
            const buffer = Buffer.from(data);
            connection.uploadStream.write(buffer);
        } catch (err) {
            console.error('写入数据错误:', err);
            socket.emit('sftp-upload-error', { sessionId, error: err.message });
        }
    });
    
    socket.on('sftp-upload-end', ({ sessionId }) => {
        const connectionId = `${socket.id}_${sessionId}`;
        const connection = sshConnections.get(connectionId);
        if (!connection || !connection.uploadStream) return;
        connection.uploadStream.end();
    });
    
    // =========================
    // 文件下载
    // =========================
    socket.on('sftp-download', ({ sessionId, remotePath }) => {
        const connectionId = `${socket.id}_${sessionId}`;
        const connection = sshConnections.get(connectionId);
        if (!connection || !connection.sftp) {
            socket.emit('sftp-download-error', { sessionId, error: 'SFTP未就绪' });
            return;
        }
        
        console.log(`开始下载: ${remotePath}`);
        
        const readStream = connection.sftp.createReadStream(remotePath);
        const chunks = [];
        
        readStream.on('data', (chunk) => {
            chunks.push(chunk);
        });
        
        readStream.on('end', () => {
            const fileData = Buffer.concat(chunks);
            const fileName = path.basename(remotePath);
            const tempPath = path.join(__dirname, 'uploads', `${Date.now()}_${fileName}`);
            
            fs.writeFile(tempPath, fileData, (err) => {
                if (err) {
                    console.error(`保存文件错误:`, err);
                    socket.emit('sftp-download-error', { sessionId, error: err.message });
                    return;
                }
                console.log(`下载完成: ${remotePath}`);
                socket.emit('sftp-download-complete', { 
                    sessionId, 
                    localPath: `/uploads/${path.basename(tempPath)}`,
                    fileName 
                });
            });
        });
        
        readStream.on('error', (err) => {
            console.error(`下载错误 ${remotePath}:`, err.message);
            socket.emit('sftp-download-error', { sessionId, error: err.message });
        });
    });
    
    // =========================
    // 列出目录
    // =========================
    socket.on('sftp-list', ({ sessionId, path: dirPath }, callback) => {
        const connectionId = `${socket.id}_${sessionId}`;
        const connection = sshConnections.get(connectionId);
        if (!connection || !connection.sftp) {
            callback({ error: 'SFTP未就绪' });
            return;
        }
        
        connection.sftp.readdir(dirPath, (err, list) => {
            if (err) {
                callback({ error: err.message });
                return;
            }
            
            const files = list.map(item => ({
                name: item.filename,
                type: item.longname.startsWith('d') ? 'directory' : 'file',
                size: item.attrs.size,
                modifyTime: item.attrs.mtime * 1000,
                permissions: item.longname
            }));
            
            callback({ files, currentPath: dirPath });
        });
    });
    
    // 删除文件/目录
    socket.on('sftp-delete', ({ sessionId, path: targetPath, isDirectory }, callback) => {
        const connectionId = `${socket.id}_${sessionId}`;
        const connection = sshConnections.get(connectionId);
        if (!connection || !connection.sftp) {
            callback({ error: 'SFTP未就绪' });
            return;
        }
        
        if (isDirectory) {
            function deleteDirectory(dirPath, sftpClient, callback) {
                sftpClient.readdir(dirPath, (err, files) => {
                    if (err) {
                        callback(err);
                        return;
                    }
                    
                    let pending = files.length;
                    if (pending === 0) {
                        sftpClient.rmdir(dirPath, callback);
                        return;
                    }
                    
                    files.forEach(file => {
                        const filePath = `${dirPath}/${file.filename}`;
                        if (file.longname.startsWith('d')) {
                            deleteDirectory(filePath, sftpClient, (err) => {
                                if (err) callback(err);
                                else if (--pending === 0) sftpClient.rmdir(dirPath, callback);
                            });
                        } else {
                            sftpClient.unlink(filePath, (err) => {
                                if (err) callback(err);
                                else if (--pending === 0) sftpClient.rmdir(dirPath, callback);
                            });
                        }
                    });
                });
            }
            
            deleteDirectory(targetPath, connection.sftp, (err) => {
                if (err) {
                    callback({ error: err.message });
                    return;
                }
                callback({ success: true });
            });
        } else {
            connection.sftp.unlink(targetPath, (err) => {
                if (err) {
                    callback({ error: err.message });
                    return;
                }
                callback({ success: true });
            });
        }
    });
    
    // 创建目录
    socket.on('sftp-mkdir', ({ sessionId, path: targetPath }, callback) => {
        const connectionId = `${socket.id}_${sessionId}`;
        const connection = sshConnections.get(connectionId);
        if (!connection || !connection.sftp) {
            callback({ error: 'SFTP未就绪' });
            return;
        }
        
        connection.sftp.mkdir(targetPath, (err) => {
            if (err) {
                callback({ error: err.message });
                return;
            }
            callback({ success: true });
        });
    });
    
    // 重命名
    socket.on('sftp-rename', ({ sessionId, oldPath, newPath }, callback) => {
        const connectionId = `${socket.id}_${sessionId}`;
        const connection = sshConnections.get(connectionId);
        if (!connection || !connection.sftp) {
            callback({ error: 'SFTP未就绪' });
            return;
        }
        
        connection.sftp.rename(oldPath, newPath, (err) => {
            if (err) {
                callback({ error: err.message });
                return;
            }
            callback({ success: true });
        });
    });

    // =========================
    // 前端请求
    // =========================
    socket.on('session-connect', ({ sessionId, config }) => {
        // 每个连接都创建独立的 SSH 会话
        createSSH(sessionId, config);
    });

    socket.on('session-disconnect', ({ sessionId }) => {
        closeSession(sessionId);
    });

    socket.on('session-command', ({ sessionId, command }) => {
        const connectionId = `${socket.id}_${sessionId}`;
        const connection = sshConnections.get(connectionId);
        if (connection && connection.sshStream && !connection.sshStream.destroyed && connection.isConnected) {
            try {
                connection.sshStream.write(command);
            } catch (e) {
                console.log(`写入失败 ${connectionId}:`, e.message);
            }
        }
    });

    socket.on('session-resize', ({ sessionId, size }) => {
        const connectionId = `${socket.id}_${sessionId}`;
        const connection = sshConnections.get(connectionId);
        if (connection && connection.sshStream) {
            try {
                connection.sshStream.setWindow(size.rows, size.cols, 0, 0);
            } catch (e) {}
        }
    });

    socket.on('disconnect', () => {
        console.log(`[${new Date().toLocaleString()}] 页面断开: ${socket.id}`);
        destroyTimer = setTimeout(() => {
            console.log(`销毁所有SSH连接: ${socket.id}`);
            // 清理该 socket 的所有连接
            for (const [connId, connection] of sshConnections) {
                if (connId.startsWith(socket.id)) {
                    if (connection.sshClient) {
                        try { connection.sshClient.end(); } catch(e) {}
                    }
                    sshConnections.delete(connId);
                }
            }
        }, 60000);
    });

    socket.on('reconnect-session', () => {
        if (destroyTimer) {
            clearTimeout(destroyTimer);
            destroyTimer = null;
        }
    });
});

const PORT = process.env.PORT || 3000;

if (!fs.existsSync('uploads')) {
    fs.mkdirSync('uploads');
}

if (!fs.existsSync(CONFIG_FILE)) {
    const defaultConfig = {
        groups: [
            { id: 1, name: '默认分组', collapsed: false },
            { id: 2, name: '生产环境', collapsed: false },
            { id: 3, name: '测试环境', collapsed: false }
        ],
        servers: [
            { id: 1, groupId: 1, name: '测试服务器1', host: '10.0.0.128', port: 22, username: 'root', password: '' },
            { id: 2, groupId: 1, name: '测试服务器2', host: '10.0.0.129', port: 22, username: 'root', password: '' }
        ],
        nextGroupId: 4,
        nextServerId: 3
    };
    fs.writeFileSync(CONFIG_FILE, JSON.stringify(defaultConfig, null, 2), 'utf8');
}

setInterval(() => {
    const uploadsDir = path.join(__dirname, 'uploads');
    fs.readdir(uploadsDir, (err, files) => {
        if (err) return;
        const now = Date.now();
        files.forEach(file => {
            const filePath = path.join(uploadsDir, file);
            fs.stat(filePath, (err, stats) => {
                if (err) return;
                if (now - stats.mtimeMs > 3600000) {
                    fs.unlink(filePath, () => {});
                }
            });
        });
    });
}, 3600000);

server.listen(PORT, '0.0.0.0', () => {
    console.log(`
╔══════════════════════════════════════════════╗
║      🚀 WebSSH 多服务器版 (支持SFTP)        ║
╠══════════════════════════════════════════════╣
║  地址: http://localhost:${PORT}
║
║  功能:
║   ✓ 支持多台服务器同时连接
║   ✓ 独立SSH会话 (互不影响)
║   ✓ SSH协议层保活
║   ✓ 自动重连
║   ✓ SFTP文件管理
║   ✓ 流式文件上传/下载
║   ✓ 目录管理
║   ✓ 配置持久化 (JSON文件)
║   ✓ 配置导入/导出
╠══════════════════════════════════════════════╣
║  配置文件: config.json
║  上传限制: 无限制
║  下载缓存: 1小时自动清理
║  独立会话: 每个连接独立
╚══════════════════════════════════════════════╝
`);
});