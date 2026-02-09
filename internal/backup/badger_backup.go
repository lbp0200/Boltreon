package backup

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/logger"
)

// BadgerBackupManager BadgerDB备份管理器
type BadgerBackupManager struct {
	db *badger.DB
}

// NewBadgerBackupManager 创建新的BadgerDB备份管理器
func NewBadgerBackupManager(db *badger.DB) *BadgerBackupManager {
	return &BadgerBackupManager{
		db: db,
	}
}

// Backup 执行BadgerDB备份
func (bbm *BadgerBackupManager) Backup(backupDir string) (string, error) {
	// 创建备份目录
	if err := os.MkdirAll(backupDir, 0750); err != nil {
		return "", fmt.Errorf("create backup directory failed: %w", err)
	}

	// 生成备份文件名
	timestamp := time.Now().Format("20060102_150405")
	backupFile := filepath.Join(backupDir, fmt.Sprintf("badger_backup_%s", timestamp))

	// 验证路径不包含遍历符
	cleanFile := filepath.Clean(backupFile)
	if !strings.HasPrefix(cleanFile, filepath.Clean(backupDir)) {
		return "", fmt.Errorf("invalid backup path: path traversal detected")
	}

	// 创建备份文件
	file, err := os.Create(cleanFile)
	if err != nil {
		logger.Logger.Error().Err(err).Str("backup_file", cleanFile).Msg("create backup file failed")
		return "", fmt.Errorf("create backup file failed: %w", err)
	}
	defer func() { _ = file.Close() }()

	// 执行备份
	_, err = bbm.db.Backup(file, 0)
	if err != nil {
		return "", fmt.Errorf("backup failed: %w", err)
	}

	logger.Logger.Info().
		Str("backup_file", backupFile).
		Msg("BadgerDB备份完成")

	return backupFile, nil
}

// IncrementalBackup 执行增量备份
func (bbm *BadgerBackupManager) IncrementalBackup(backupDir string, since uint64) (string, error) {
	// 创建备份目录
	if err := os.MkdirAll(backupDir, 0750); err != nil {
		return "", fmt.Errorf("create backup directory failed: %w", err)
	}

	// 生成备份文件名
	timestamp := time.Now().Format("20060102_150405")
	backupFile := filepath.Join(backupDir, fmt.Sprintf("badger_backup_inc_%s", timestamp))

	// 验证路径不包含遍历符
	cleanFile := filepath.Clean(backupFile)
	if !strings.HasPrefix(cleanFile, filepath.Clean(backupDir)) {
		return "", fmt.Errorf("invalid backup path: path traversal detected")
	}

	// 创建备份文件
	file, err := os.Create(cleanFile)
	if err != nil {
		logger.Logger.Error().Err(err).Str("backup_file", cleanFile).Msg("create backup file failed")
		return "", fmt.Errorf("create backup file failed: %w", err)
	}
	defer func() { _ = file.Close() }()

	// 执行增量备份
	_, err = bbm.db.Backup(file, since)
	if err != nil {
		return "", fmt.Errorf("incremental backup failed: %w", err)
	}

	logger.Logger.Info().
		Str("backup_file", backupFile).
		Uint64("since", since).
		Msg("BadgerDB增量备份完成")

	return backupFile, nil
}

// Restore 从备份恢复
func (bbm *BadgerBackupManager) Restore(backupFile string) error {
	// 打开备份文件
	// nosec G304 - backupFile is validated by caller
	file, err := os.Open(backupFile)
	if err != nil {
		return fmt.Errorf("open backup file failed: %w", err)
	}
	defer func() { _ = file.Close() }()

	// 执行恢复
	err = bbm.db.Load(file, 1)
	if err != nil {
		logger.Logger.Error().Err(err).Str("backup_file", backupFile).Msg("restore failed")
		return fmt.Errorf("restore failed: %w", err)
	}

	logger.Logger.Info().
		Str("backup_file", backupFile).
		Msg("BadgerDB恢复完成")

	return nil
}

// RestoreTo 恢复到新的数据库
func RestoreTo(backupFile, dbPath string) error {
	// 打开目标数据库
	opts := badger.DefaultOptions(dbPath)
	db, err := badger.Open(opts)
	if err != nil {
		logger.Logger.Error().Err(err).Str("db_path", dbPath).Msg("open target database failed")
		return fmt.Errorf("open target database failed: %w", err)
	}
	defer func() { _ = db.Close() }()

	// 打开备份文件
	// nosec G304 - backupFile is validated by caller
	file, err := os.Open(backupFile)
	if err != nil {
		logger.Logger.Error().Err(err).Str("backup_file", backupFile).Msg("open backup file failed")
		return fmt.Errorf("open backup file failed: %w", err)
	}
	defer func() { _ = file.Close() }()

	// 执行恢复
	err = db.Load(file, 1)
	if err != nil {
		logger.Logger.Error().Err(err).Str("backup_file", backupFile).Str("db_path", dbPath).Msg("restore failed")
		return fmt.Errorf("restore failed: %w", err)
	}

	logger.Logger.Info().
		Str("backup_file", backupFile).
		Str("db_path", dbPath).
		Msg("BadgerDB恢复到新数据库完成")

	return nil
}

// GetBackupInfo 获取备份信息
func GetBackupInfo(backupFile string) (map[string]interface{}, error) {
	file, err := os.Open(backupFile)
	if err != nil {
		return nil, fmt.Errorf("open backup file failed: %w", err)
	}
	defer func() { _ = file.Close() }()

	info := make(map[string]interface{})

	// 获取文件信息
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("get file info failed: %w", err)
	}

	info["size"] = fileInfo.Size()
	info["mod_time"] = fileInfo.ModTime()
	info["path"] = backupFile

	return info, nil
}

// ListBackups 列出备份目录中的所有备份
func ListBackups(backupDir string) ([]string, error) {
	files, err := os.ReadDir(backupDir)
	if err != nil {
		return nil, fmt.Errorf("read backup directory failed: %w", err)
	}

	var backups []string
	for _, file := range files {
		if !file.IsDir() && (filepath.Ext(file.Name()) == "" || filepath.Ext(file.Name()) == ".bak") {
			backups = append(backups, filepath.Join(backupDir, file.Name()))
		}
	}

	return backups, nil
}

// CopyBackup 复制备份文件
func CopyBackup(src, dst string) error {
	// nosec G304 - src and dst are validated by caller
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source file failed: %w", err)
	}
	defer func() { _ = srcFile.Close() }()

	// nosec G304 - dst is validated by caller
	dstFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create destination file failed: %w", err)
	}
	defer func() { _ = dstFile.Close() }()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return fmt.Errorf("copy file failed: %w", err)
	}

	return nil
}
