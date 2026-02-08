package cluster

import (
	"os"
	"strings"
	"testing"

	"github.com/lbp0200/Botreon/internal/store"
	"github.com/zeebo/assert"
)

func setupTestCluster(t *testing.T) (*Cluster, func()) {
	dbPath := t.TempDir()
	s, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)

	cluster, err := NewCluster(s, "", "127.0.0.1:6379")
	assert.NoError(t, err)

	return cluster, func() {
		assert.NoError(t, s.Close())
		assert.NoError(t, os.RemoveAll(dbPath))
	}
}

func TestSlotCalculation(t *testing.T) {
	// 测试基本槽位计算
	slot := Slot("testkey")
	assert.True(t, slot < SlotCount)

	// 测试hash tag
	slot1 := Slot("user{1000}:name")
	slot2 := Slot("user{1000}:age")
	assert.Equal(t, slot1, slot2) // 相同hash tag应该映射到相同槽位

	// 测试不同hash tag
	slot3 := Slot("user{1000}:name")
	slot4 := Slot("user{2000}:name")
	assert.NotEqual(t, slot3, slot4)
}

func TestClusterCreation(t *testing.T) {
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	myself := cluster.GetMyself()
	assert.NotNil(t, myself)
	assert.True(t, myself.IsMyself())
	assert.True(t, myself.IsMaster())
	assert.Equal(t, 40, len(myself.ID)) // 节点ID应该是40字符
}

func TestSlotAssignment(t *testing.T) {
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// 创建新节点
	nodeID, _ := generateNodeID()
	node := NewNode(nodeID, "127.0.0.1:6380")
	node.Flags = append(node.Flags, "master")
	cluster.AddNode(node)

	// 分配槽位给新节点
	err := cluster.AssignSlot(100, nodeID)
	assert.NoError(t, err)

	owner := cluster.GetNodeBySlot(100)
	assert.NotNil(t, owner)
	assert.Equal(t, nodeID, owner.ID)
}

func TestSlotRangeAssignment(t *testing.T) {
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	nodeID, _ := generateNodeID()
	node := NewNode(nodeID, "127.0.0.1:6380")
	node.Flags = append(node.Flags, "master")
	cluster.AddNode(node)

	// 分配槽位范围
	err := cluster.AssignSlotRange(0, 100, nodeID)
	assert.NoError(t, err)

	// 验证槽位分配
	for i := uint32(0); i <= 100; i++ {
		owner := cluster.GetNodeBySlot(i)
		assert.NotNil(t, owner)
		assert.Equal(t, nodeID, owner.ID)
	}
}

func TestIsSlotLocal(t *testing.T) {
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// 初始时所有槽位都属于当前节点
	assert.True(t, cluster.IsSlotLocal(0))
	assert.True(t, cluster.IsSlotLocal(1000))
	assert.True(t, cluster.IsSlotLocal(SlotCount-1))

	// 分配槽位给其他节点后，应该返回false
	nodeID, _ := generateNodeID()
	node := NewNode(nodeID, "127.0.0.1:6380")
	cluster.AddNode(node)
	cluster.AssignSlot(100, nodeID)

	assert.False(t, cluster.IsSlotLocal(100))
}

func TestClusterCommands(t *testing.T) {
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// 测试CLUSTER INFO
	info, err := cmd.HandleCommand([]string{"INFO"})
	assert.NoError(t, err)
	assert.NotNil(t, info)
	infoStr, ok := info.(string)
	assert.True(t, ok)
	assert.True(t, strings.Contains(infoStr, "cluster_state:ok"))

	// 测试CLUSTER MYID
	myid, err := cmd.HandleCommand([]string{"MYID"})
	assert.NoError(t, err)
	myidStr, ok := myid.(string)
	assert.True(t, ok)
	assert.Equal(t, 40, len(myidStr))

	// 测试CLUSTER KEYSLOT
	keyslot, err := cmd.HandleCommand([]string{"KEYSLOT", "testkey"})
	assert.NoError(t, err)
	slot, ok := keyslot.(int64)
	assert.True(t, ok)
	assert.True(t, slot >= 0 && slot < SlotCount)

	// 测试CLUSTER NODES
	nodes, err := cmd.HandleCommand([]string{"NODES"})
	assert.NoError(t, err)
	nodesStr, ok := nodes.(string)
	assert.True(t, ok)
	assert.True(t, strings.Contains(nodesStr, cluster.GetMyself().ID))

	// 测试CLUSTER SLOTS
	slots, err := cmd.HandleCommand([]string{"SLOTS"})
	assert.NoError(t, err)
	slotsArr, ok := slots.([][]interface{})
	assert.True(t, ok)
	assert.True(t, len(slotsArr) > 0)
}

func TestClusterMeet(t *testing.T) {
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// 测试CLUSTER MEET
	result, err := cmd.HandleCommand([]string{"MEET", "127.0.0.1", "6380"})
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// 验证节点已添加
	assert.True(t, len(cluster.Nodes) > 1)
}

func TestClusterAddSlots(t *testing.T) {
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()
	cmd := NewClusterCommands(cluster)

	// 创建新节点并添加到集群
	nodeID, _ := generateNodeID()
	node := NewNode(nodeID, "127.0.0.1:6380")
	cluster.AddNode(node)

	// 测试CLUSTER ADDSLOTS
	result, err := cmd.HandleCommand([]string{"ADDSLOTS", "100", "101", "102"})
	assert.NoError(t, err)
	assert.Equal(t, "OK", result)

	// 注意：ADDSLOTS会将槽位分配给当前节点，不是新节点
	// 这里只是测试命令执行成功
}

func TestRedirectError(t *testing.T) {
	cluster, cleanup := setupTestCluster(t)
	defer cleanup()

	// 创建新节点并分配槽位
	nodeID, _ := generateNodeID()
	node := NewNode(nodeID, "127.0.0.1:6380")
	cluster.AddNode(node)
	cluster.AssignSlot(100, nodeID)

	// 测试重定向检查
	redirect := cluster.CheckSlotRedirect("testkey")
	slot := Slot("testkey")
	if slot == 100 {
		// 如果槽位恰好是100，应该返回重定向
		assert.NotNil(t, redirect)
	} else {
		// 否则应该返回nil（槽位属于当前节点）
		// 或者如果槽位被分配给其他节点，返回重定向
	}
}

