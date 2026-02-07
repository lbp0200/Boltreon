package main

import (
	"flag"
	"net"
	"time"

	"github.com/lbp0200/Boltreon/internal/logger"
	"github.com/lbp0200/Boltreon/internal/sentinel"
)

func main() {
	addr := flag.String("addr", ":26379", "sentinel listen addr")
	quorum := flag.Int("quorum", 2, "quorum for failover")
	downAfter := flag.Duration("down-after", 30*time.Second, "down after duration")
	logLevel := flag.String("log-level", "", "log level: DEBUG, INFO, WARNING, ERROR")
	flag.Parse()

	// 设置日志级别
	if *logLevel != "" {
		logger.SetLevelFromString(*logLevel)
	}

	// 创建哨兵实例
	s := sentinel.NewSentinel(*quorum, *downAfter)

	// 启动哨兵
	s.Start()
	defer s.Stop()

	// 监听客户端连接
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Logger.Fatal().Err(err).Str("addr", *addr).Msg("Failed to listen")
	}

	logger.Warning("哨兵服务器启动，监听地址: %s", *addr)

	// 创建命令处理器
	handler := sentinel.NewSentinelHandler(s)

	// 接受连接
	for {
		conn, err := ln.Accept()
		if err != nil {
			logger.Logger.Error().Err(err).Msg("Accept connection failed")
			continue
		}

		go handler.HandleConnection(conn)
	}
}
