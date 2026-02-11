package server

import (
	"fmt"
	"strings"
)

// buildInfoResponse 构建INFO响应
// 增强对 redis-sentinel 的兼容性
func (h *Handler) buildInfoResponse(section string) string {
	var builder strings.Builder

	if section == "" || section == "ALL" || section == "SERVER" {
		builder.WriteString("# Server\n")
		builder.WriteString("redis_version:boltdb-8.0.0\n")
		builder.WriteString("os:linux\n")
		builder.WriteString("arch_bits:64\n")
		builder.WriteString("tcp_port:6379\n")
		builder.WriteString("multiplexing_api:epoll\n")
		builder.WriteString("gcc_version:go1.25\n")
		builder.WriteString("process_id:1\n")
		builder.WriteString("run_id:\n")
		builder.WriteString("tcp_backlog:511\n")
		builder.WriteString("uptime_in_seconds:0\n")
		builder.WriteString("uptime_in_days:0\n")
		builder.WriteString("\n")
	}

	if section == "" || section == "ALL" || section == "REPLICATION" {
		builder.WriteString("# Replication\n")
		if h.Replication != nil {
			role := h.Replication.GetRole()
			builder.WriteString(fmt.Sprintf("role:%s\n", role))

			if role == "master" {
				builder.WriteString(fmt.Sprintf("connected_slaves:%d\n", h.Replication.GetSlaveCount()))
				builder.WriteString(fmt.Sprintf("master_replid:%s\n", h.Replication.GetReplicationID()))
				builder.WriteString(fmt.Sprintf("master_repl_offset:%d\n", h.Replication.GetMasterReplOffset()))
				builder.WriteString(fmt.Sprintf("second_repl_offset:-1\n"))
				builder.WriteString(fmt.Sprintf("repl_backlog_active:%d\n", 1))
				builder.WriteString(fmt.Sprintf("repl_backlog_size:%d\n", 1048576)) // 1MB
				builder.WriteString(fmt.Sprintf("repl_backlog_first_byte_offset:%d\n", 0))
				builder.WriteString(fmt.Sprintf("repl_backlog_histlen:%d\n", 0))

				slaves := h.Replication.GetSlaves()
				for i, slave := range slaves {
					builder.WriteString(fmt.Sprintf("slave%d:ip=%s,port=6379,state=online,offset=%d,lag=0\n",
						i, slave.Addr, slave.GetReplOffset()))
				}
			} else if role == "slave" {
				masterAddr := h.Replication.GetMasterAddr()
				if masterAddr != "" {
					builder.WriteString(fmt.Sprintf("master_host:%s\n", strings.Split(masterAddr, ":")[0]))
					if parts := strings.Split(masterAddr, ":"); len(parts) > 1 {
						builder.WriteString(fmt.Sprintf("master_port:%s\n", parts[1]))
					}
				}
				builder.WriteString(fmt.Sprintf("master_link_status:%s\n", "up"))
				builder.WriteString(fmt.Sprintf("master_link_down_since_seconds:0\n"))
				builder.WriteString(fmt.Sprintf("slave_priority:100\n"))
				builder.WriteString(fmt.Sprintf("slave_read_only:1\n"))
				builder.WriteString(fmt.Sprintf("replica_announced:1\n"))
				builder.WriteString(fmt.Sprintf("connected_slaves:0\n"))
				builder.WriteString(fmt.Sprintf("master_replid:%s\n", h.Replication.GetReplicationID()))
				builder.WriteString(fmt.Sprintf("master_repl_offset:%d\n", h.Replication.GetMasterReplOffset()))
				builder.WriteString(fmt.Sprintf("second_repl_offset:-1\n"))
				builder.WriteString(fmt.Sprintf("repl_backlog_active:0\n"))
				builder.WriteString(fmt.Sprintf("repl_backlog_size:0\n"))
				builder.WriteString(fmt.Sprintf("repl_backlog_first_byte_offset:0\n"))
				builder.WriteString(fmt.Sprintf("repl_backlog_histlen:0\n"))
			}
		} else {
			builder.WriteString("role:master\n")
			builder.WriteString("connected_slaves:0\n")
			builder.WriteString("master_replid:8371b4fb5d6973276c54b0f0ab738c2e6f00fa8d\n")
			builder.WriteString("master_repl_offset:0\n")
			builder.WriteString("second_repl_offset:-1\n")
			builder.WriteString("repl_backlog_active:0\n")
		}
		builder.WriteString("\n")
	}

	if section == "" || section == "ALL" || section == "PERSISTENCE" {
		builder.WriteString("# Persistence\n")
		if h.Backup != nil {
			lastSave := h.Backup.LastSave()
			builder.WriteString(fmt.Sprintf("rdb_last_save_time:%d\n", lastSave))
			builder.WriteString("rdb_changes_since_last_save:0\n")
		}
		builder.WriteString("\n")
	}

	if section == "" || section == "ALL" || section == "STATS" {
		builder.WriteString("# Stats\n")
		builder.WriteString("total_commands_processed:0\n")
		builder.WriteString("instantaneous_ops_per_sec:0\n")
		builder.WriteString("\n")
	}

	if section == "" || section == "ALL" || section == "CLUSTER" {
		builder.WriteString("# Cluster\n")
		if h.Cluster != nil {
			builder.WriteString("cluster_enabled:1\n")
		} else {
			builder.WriteString("cluster_enabled:0\n")
		}
		builder.WriteString("\n")
	}

	return builder.String()
}
