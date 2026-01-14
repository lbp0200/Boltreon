package backup

import (
	"sync"
	"time"

	"github.com/lbp0200/Boltreon/internal/store"
)

// BackupManager 统一的备份管理器
type BackupManager struct {
	store           *store.BoltreonStore
	badgerMgr       *BadgerBackupManager
	rdbMgr          *RDBBackupManager
	lastSaveTime    int64
	lastSaveTimeMu  sync.RWMutex
	backupDir       string
}

// NewBackupManager 创建新的备份管理器
func NewBackupManager(store *store.BoltreonStore, backupDir string) *BackupManager {
	return &BackupManager{
		store:     store,
		badgerMgr: NewBadgerBackupManager(store.GetDB()),
		rdbMgr:    NewRDBBackupManager(store),
		backupDir: backupDir,
	}
}

// Save 同步保存RDB
func (bm *BackupManager) Save() error {
	backupFile, err := bm.rdbMgr.Backup(bm.backupDir)
	if err != nil {
		return err
	}

	bm.lastSaveTimeMu.Lock()
	bm.lastSaveTime = time.Now().Unix()
	bm.lastSaveTimeMu.Unlock()

	_ = backupFile // 记录备份文件路径
	return nil
}

// BGSave 后台保存RDB
func (bm *BackupManager) BGSave() error {
	// 在goroutine中执行备份
	go func() {
		if err := bm.Save(); err != nil {
			// 记录错误日志
			return
		}
	}()

	return nil
}

// LastSave 获取最后保存时间
func (bm *BackupManager) LastSave() int64 {
	bm.lastSaveTimeMu.RLock()
	defer bm.lastSaveTimeMu.RUnlock()
	return bm.lastSaveTime
}

// BackupBadger 执行BadgerDB备份
func (bm *BackupManager) BackupBadger() (string, error) {
	return bm.badgerMgr.Backup(bm.backupDir)
}

// BackupRDB 执行RDB备份
func (bm *BackupManager) BackupRDB() (string, error) {
	return bm.rdbMgr.Backup(bm.backupDir)
}

// BackupBoth 同时执行两种格式的备份
func (bm *BackupManager) BackupBoth() ([]string, error) {
	var files []string

	// BadgerDB备份
	badgerFile, err := bm.badgerMgr.Backup(bm.backupDir)
	if err != nil {
		return nil, err
	}
	files = append(files, badgerFile)

	// RDB备份
	rdbFile, err := bm.rdbMgr.Backup(bm.backupDir)
	if err != nil {
		return nil, err
	}
	files = append(files, rdbFile)

	return files, nil
}
