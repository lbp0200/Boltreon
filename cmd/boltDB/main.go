package main

import (
	"flag"
	"net"
	"os"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/server"

	"github.com/lbp0200/BoltDB/internal/store"
)

func main() {
	addr := flag.String("addr", ":6379", "listen addr")
	dbPath := flag.String("dir", os.TempDir(), "badger dir")
	logLevel := flag.String("log-level", "", "log level: DEBUG, INFO, WARNING, ERROR (default: WARNING, or from BOLTREON_LOG_LEVEL env)")
	flag.Parse()

	// 设置日志级别
	if *logLevel != "" {
		logger.SetLevelFromString(*logLevel)
	}

	db, err := store.NewBotreonStore(*dbPath)
	if err != nil {
		logger.Logger.Fatal().Err(err).Msg("Failed to create store")
	}
	defer db.Close()

	// 初始化复制管理器
	replMgr := replication.NewReplicationManager(db)

	// 初始化备份管理器
	backupDir := *dbPath + "/backup"
	backupMgr := backup.NewBackupManager(db, backupDir)

	// 初始化Pub/Sub管理器
	pubsubMgr := store.NewPubSubManager()

	handler := &server.Handler{
		Db:          db,
		Replication: replMgr,
		Backup:      backupMgr,
		PubSub:      pubsubMgr,
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Logger.Fatal().Err(err).Str("addr", *addr).Msg("Failed to listen")
	}
	// 启动信息使用 WARN 级别，确保默认配置下也能显示
	logger.Warning("Botreon 服务器启动，监听地址: %s", *addr)
	logger.Warning("当前日志级别: %s", logger.GetLevelString())
	if err := handler.ServeTCP(ln); err != nil {
		logger.Logger.Fatal().Err(err).Msg("Server failed")
	}
}
