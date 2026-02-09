package replication

import (
	"fmt"
	"strings"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

// PSyncResult PSYNC结果
type PSyncResult struct {
	FullResync bool   // 是否全量同步
	ReplId     string // 复制ID
	Offset     int64  // 复制偏移量
}

// HandlePSync 处理PSYNC命令（主节点端）
func HandlePSync(rm *ReplicationManager, replId string, offset int64) (*PSyncResult, error) {
	rm.mu.RLock()
	currentReplId := rm.replId
	currentOffset := rm.masterReplOffset
	backlog := rm.backlog
	rm.mu.RUnlock()

	// 检查是否可以增量同步
	if replId == currentReplId && offset > 0 {
		// 检查backlog中是否有足够的数据
		backlogStart := backlog.GetCurrentOffset() - backlog.GetSize()
		if backlogStart < 0 {
			backlogStart = 0
		}

		if offset >= backlogStart && offset < currentOffset {
			// 可以增量同步
			logger.Logger.Info().
				Str("repl_id", replId).
				Int64("offset", offset).
				Msg("执行增量同步")
			return &PSyncResult{
				FullResync: false,
				ReplId:     currentReplId,
				Offset:     offset,
			}, nil
		}
	}

	// 需要全量同步
	logger.Logger.Info().
		Str("requested_repl_id", replId).
		Str("current_repl_id", currentReplId).
		Int64("requested_offset", offset).
		Int64("current_offset", currentOffset).
		Msg("执行全量同步")
	return &PSyncResult{
		FullResync: true,
		ReplId:     currentReplId,
		Offset:     currentOffset,
	}, nil
}

// SendFullResync 发送全量同步响应
func SendFullResync(slave *SlaveConnection, replId string, offset int64) error {
	// 发送 +FULLRESYNC <replid> <offset>
	response := fmt.Sprintf("+FULLRESYNC %s %d\r\n", replId, offset)
	if err := slave.SendResponse(proto.NewSimpleString(strings.TrimSpace(response))); err != nil {
		return fmt.Errorf("send FULLRESYNC response failed: %w", err)
	}
	return nil
}

// SendContinueResync 发送增量同步响应
func SendContinueResync(slave *SlaveConnection, replId string, offset int64) error {
	// 发送 +CONTINUE <replid>
	response := fmt.Sprintf("+CONTINUE %s\r\n", replId)
	if err := slave.SendResponse(proto.NewSimpleString(strings.TrimSpace(response))); err != nil {
		return fmt.Errorf("send CONTINUE response failed: %w", err)
	}
	return nil
}

// SendBacklogData 发送backlog数据到从节点
func SendBacklogData(slave *SlaveConnection, backlog *ReplicationBacklog, startOffset, endOffset int64) error {
	data, err := backlog.GetRange(startOffset, endOffset)
	if err != nil {
		return fmt.Errorf("get backlog range failed: %w", err)
	}

	if len(data) == 0 {
		return nil
	}

	// 发送数据
	if _, err := slave.Writer.Write(data); err != nil {
		return fmt.Errorf("write backlog data failed: %w", err)
	}

	if err := slave.Writer.Flush(); err != nil {
		return fmt.Errorf("flush backlog data failed: %w", err)
	}

	logger.Logger.Debug().
		Str("slave_id", slave.ID).
		Int64("start_offset", startOffset).
		Int64("end_offset", endOffset).
		Int("data_size", len(data)).
		Msg("发送backlog数据到从节点")

	return nil
}

// StartSlaveReplication 启动从节点复制（从节点端）
func StartSlaveReplication(rm *ReplicationManager, store *store.BotreonStore, masterAddr string) error {
	rm.mu.Lock()
	rm.role = RoleSlave
	rm.masterAddr = masterAddr
	rm.mu.Unlock()

	// 连接到主节点
	masterConn, err := NewMasterConnection(masterAddr)
	if err != nil {
		return fmt.Errorf("connect to master failed: %w", err)
	}

	rm.SetMasterConnection(masterConn)

	// 启动复制goroutine
	go func() {
		defer func() { _ = masterConn.Close() }()

		// 发送PING
		if err := masterConn.SendCommand([][]byte{[]byte("PING")}); err != nil {
			logger.Logger.Error().Err(err).Msg("发送PING到主节点失败")
			return
		}

		resp, err := masterConn.ReadResponse()
		if err != nil {
			logger.Logger.Error().Err(err).Msg("读取PING响应失败")
			return
		}

		if _, ok := resp.(*proto.SimpleString); !ok || resp.String() != "+PONG\r\n" {
			logger.Logger.Error().Msg("主节点PING响应异常")
			return
		}

		// 发送REPLCONF listening-port
		// 简化实现，不发送实际端口

		// 发送REPLCONF capa
		if err := masterConn.SendCommand([][]byte{
			[]byte("REPLCONF"),
			[]byte("capa"),
			[]byte("eof"),
			[]byte("capa"),
			[]byte("psync2"),
		}); err != nil {
			logger.Logger.Error().Err(err).Msg("发送REPLCONF capa失败")
			return
		}

		_, _ = masterConn.ReadResponse()

		// 发送PSYNC
		if err := masterConn.SendCommand([][]byte{
			[]byte("PSYNC"),
			[]byte("?"),
			[]byte("-1"),
		}); err != nil {
			logger.Logger.Error().Err(err).Msg("发送PSYNC失败")
			return
		}

		// 读取PSYNC响应
		resp, err = masterConn.ReadResponse()
		if err != nil {
			logger.Logger.Error().Err(err).Msg("读取PSYNC响应失败")
			return
		}

		respStr := resp.String()
		logger.Logger.Info().Str("psync_response", respStr).Msg("收到PSYNC响应")

		// 解析响应
		if strings.HasPrefix(respStr, "+FULLRESYNC") {
			// 全量同步
			// 解析replid和offset
			parts := strings.Fields(respStr)
			if len(parts) >= 3 {
				newReplId := parts[1]
				rm.mu.Lock()
				rm.replId = newReplId
				rm.mu.Unlock()
			}

			// 读取RDB数据
			rdbData, err := masterConn.ReadBulkString()
			if err != nil {
				logger.Logger.Error().Err(err).Msg("读取RDB数据失败")
				return
			}

			logger.Logger.Info().
				Int("rdb_size", len(rdbData)).
				Msg("收到RDB数据，开始加载")

			// 加载RDB数据（简化实现，实际应该解析RDB）
			// 这里暂时跳过，后续可以实现RDB解析

		} else if strings.HasPrefix(respStr, "+CONTINUE") {
			// 增量同步
			parts := strings.Fields(respStr)
			if len(parts) >= 2 {
				newReplId := parts[1]
				rm.mu.Lock()
				rm.replId = newReplId
				rm.mu.Unlock()
			}

			// 开始接收命令流
			logger.Logger.Info().Msg("开始增量同步，接收命令流")
		}

		// 持续接收命令并执行
		for {
			req, err := proto.ReadRESP(masterConn.Reader)
			if err != nil {
				logger.Logger.Warn().Err(err).Msg("从主节点读取命令失败")
				break
			}

			// 执行命令（简化实现）
			// 实际应该解析命令并执行
			logger.Logger.Debug().
				Int("arg_count", len(req.Args)).
				Msg("从主节点收到命令")

			// 更新复制偏移量
			cmdBytes := serializeCommand(req.Args)
			rm.IncrementReplOffset(int64(len(cmdBytes)))
		}

		logger.Logger.Info().Msg("从节点复制连接关闭")
	}()

	return nil
}

// StopSlaveReplication 停止从节点复制
func StopSlaveReplication(rm *ReplicationManager) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.role = RoleMaster
	rm.masterAddr = ""

	if rm.masterConn != nil {
		_ = rm.masterConn.Close()
		rm.masterConn = nil
	}
}
