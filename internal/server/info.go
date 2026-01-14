package server

import (
	"fmt"
	"strings"
)

// buildInfoResponse 构建INFO响应
func (h *Handler) buildInfoResponse(section string) string {
	var builder strings.Builder

	if section == "" || section == "ALL" || section == "SERVER" {
		builder.WriteString("# Server\n")
		builder.WriteString("redis_version:boltreon-1.0.0\n")
		builder.WriteString("os:linux\n")
		builder.WriteString("arch_bits:64\n")
		builder.WriteString("tcp_port:6379\n")
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
				
				slaves := h.Replication.GetSlaves()
				for i, slave := range slaves {
					builder.WriteString(fmt.Sprintf("slave%d:ip=%s,port=0,state=online,offset=%d,lag=0\n",
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
				builder.WriteString(fmt.Sprintf("slave_repl_offset:%d\n", h.Replication.GetMasterReplOffset()))
				builder.WriteString("master_link_status:up\n")
			}
		} else {
			builder.WriteString("role:master\n")
			builder.WriteString("connected_slaves:0\n")
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
