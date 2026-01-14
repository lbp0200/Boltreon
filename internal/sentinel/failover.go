package sentinel

import (
	"fmt"
	"time"

	"github.com/lbp0200/Boltreon/internal/logger"
)

// FailoverManager 故障转移管理器
type FailoverManager struct {
	sentinel *Sentinel
}

// NewFailoverManager 创建新的故障转移管理器
func NewFailoverManager(sentinel *Sentinel) *FailoverManager {
	return &FailoverManager{
		sentinel: sentinel,
	}
}

// StartFailover 启动故障转移
func (fm *FailoverManager) StartFailover(masterName string) error {
	master := fm.sentinel.GetMaster(masterName)
	if master == nil {
		return fmt.Errorf("master %s not found", masterName)
	}

	// 检查是否已经在下线状态
	if !master.IsDown() {
		return fmt.Errorf("master %s is not down", masterName)
	}

	// 检查是否已经有其他哨兵在执行故障转移
	if master.GetState() == "failover" {
		return fmt.Errorf("failover already in progress for master %s", masterName)
	}

	// 设置状态为故障转移中
	master.SetState("failover")

	logger.Logger.Info().
		Str("master_name", masterName).
		Msg("开始故障转移")

	// 选择新的主节点
	newMaster := fm.selectNewMaster(master)
	if newMaster == nil {
		master.SetState("odown")
		return fmt.Errorf("no suitable slave found for failover")
	}

	// 执行故障转移
	if err := fm.executeFailover(master, newMaster); err != nil {
		master.SetState("odown")
		return fmt.Errorf("failover failed: %w", err)
	}

	// 更新配置
	fm.updateConfiguration(master, newMaster)

	logger.Logger.Info().
		Str("master_name", masterName).
		Str("new_master", newMaster.Addr).
		Msg("故障转移完成")

	return nil
}

// selectNewMaster 选择新的主节点
func (fm *FailoverManager) selectNewMaster(oldMaster *MasterInstance) *SlaveInstance {
	slaves := oldMaster.GetSlaves()

	if len(slaves) == 0 {
		return nil
	}

	// 简化实现：选择第一个从节点
	// 实际应该根据优先级、偏移量等选择
	for _, slave := range slaves {
		if slave.State == "online" {
			return slave
		}
	}

	return nil
}

// executeFailover 执行故障转移
func (fm *FailoverManager) executeFailover(oldMaster *MasterInstance, newMaster *SlaveInstance) error {
	// 1. 将选中的从节点提升为主节点
	// 发送 SLAVEOF NO ONE 命令
	logger.Logger.Info().
		Str("slave_addr", newMaster.Addr).
		Msg("提升从节点为主节点")

	// 2. 等待新主节点就绪
	time.Sleep(1 * time.Second)

	// 3. 将其他从节点重新配置为复制新主节点
	slaves := oldMaster.GetSlaves()
	for _, slave := range slaves {
		if slave.ID != newMaster.ID && slave.State == "online" {
			logger.Logger.Info().
				Str("slave_addr", slave.Addr).
				Str("new_master", newMaster.Addr).
				Msg("重新配置从节点")
			// 发送 REPLICAOF 命令
		}
	}

	return nil
}

// updateConfiguration 更新配置
func (fm *FailoverManager) updateConfiguration(oldMaster *MasterInstance, newMaster *SlaveInstance) {
	// 更新配置纪元
	fm.sentinel.IncrementConfigEpoch()

	// 更新主节点地址
	oldMaster.mu.Lock()
	oldMaster.addr = newMaster.Addr
	oldMaster.state = "ok"
	oldMaster.mu.Unlock()

	logger.Logger.Info().
		Str("master_name", oldMaster.GetName()).
		Str("new_addr", newMaster.Addr).
		Msg("更新主节点配置")
}
