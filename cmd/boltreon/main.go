package main

import (
	"flag"
	"net"
	"os"

	"github.com/lbp0200/Boltreon/internal/logger"
	"github.com/lbp0200/Boltreon/internal/server"

	"github.com/lbp0200/Boltreon/internal/store"
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

	db, err := store.NewBoltreonStore(*dbPath)
	if err != nil {
		logger.Logger.Fatal().Err(err).Msg("Failed to create store")
	}
	defer db.Close()

	handler := &server.Handler{Db: db}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Logger.Fatal().Err(err).Str("addr", *addr).Msg("Failed to listen")
	}
	// 启动信息使用 WARN 级别，确保默认配置下也能显示
	logger.Warning("Boltreon 服务器启动，监听地址: %s", *addr)
	logger.Warning("当前日志级别: %s", logger.GetLevelString())
	if err := handler.ServeTCP(ln); err != nil {
		logger.Logger.Fatal().Err(err).Msg("Server failed")
	}
}
