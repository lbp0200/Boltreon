package store

import (
	"time"

	"github.com/dgraph-io/badger/v4"
)

const (
	//UnderScore       = "_"
	KeyTypeString = "STRING"
	KeyTypeList   = "LIST"
	KeyTypeHash   = "HASH"
	KeyTypeSet    = "SET"
	//KeyTypeSortedSet = "SORTEDSET"
	//
	//sortedSetIndex = "_INDEX_"
	//sortedSetData  = "_DATA_"
)

var (
	prefixKeyTypeBytes   = []byte("TYPE_")
	prefixKeyStringBytes = []byte("STRING_")
	prefixKeyListBytes   = []byte("LIST_")
	prefixKeyHashBytes   = []byte("HASH_")
	prefixKeySetBytes    = []byte("SET_")
	//prefixKeySortedSetBytes = []byte("SORTEDSET_")
)

type BoltreonStore struct {
	db              *badger.DB
	compressionType CompressionType
	// 缓存层
	readCache  *LRUCache // 读缓存（用于 GET、HGET 等读操作）
	writeCache *LRUCache // 写缓存（用于 SET、HSET 等写操作，减少磁盘写入）
}

// NewBoltreonStore 创建新的BoltreonStore实例
func NewBoltreonStore(path string) (*BoltreonStore, error) {
	return NewBoltreonStoreWithCompression(path, CompressionLZ4)
}

// NewBoltreonStoreWithCompression 创建新的BoltreonStore实例，指定压缩算法
func NewBoltreonStoreWithCompression(path string, compressionType CompressionType) (*BoltreonStore, error) {
	opts := badger.DefaultOptions(path)

	// 性能优化配置
	// 1. 增加 memtable 数量，提高写入并发性能（优化：从5增加到7，减少事务冲突）
	opts.NumMemtables = 7             // 增加到 7，提高并发写入性能
	opts.NumLevelZeroTables = 5       // Level 0 表数量
	opts.NumLevelZeroTablesStall = 10 // Level 0 停滞阈值

	// 2. 优化 Value Log 配置
	opts.ValueLogFileSize = 1024 * 1024 * 1024 // 1GB（默认 1GB，适合大值）
	opts.ValueLogMaxEntries = 1000000          // 每个 vlog 文件最大条目数

	// 3. 优化 Table 配置
	// BadgerDB v4 使用 BlockSize 而不是 MaxTableSize
	opts.BlockSize = 4 * 1024     // 4KB 块大小（默认 4KB）
	opts.LevelSizeMultiplier = 10 // Level 大小倍数（默认 10）

	// 4. 压缩配置（BadgerDB 内置压缩）
	// BadgerDB v4 使用 CompressionType，值为 0=无压缩, 1=Snappy, 2=ZSTD
	opts.Compression = 2 // 使用 ZSTD 压缩（比 Snappy 更好）

	// 5. 索引缓存配置
	opts.IndexCacheSize = 100 * 1024 * 1024 // 100MB 索引缓存（默认 0，禁用）

	// 6. 减少同步频率（提高性能，但降低持久性）
	// opts.SyncWrites = false // 默认 false，异步写入提高性能

	// 7. 优化垃圾回收
	opts.NumGoroutines = 8 // GC goroutine 数量（默认 8）

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	// 初始化缓存层
	// 读缓存：10000 个条目，TTL 5 分钟
	readCache := NewLRUCache(10000, 5*time.Minute)
	// 写缓存：5000 个条目，TTL 1 分钟（用于批量写入优化）
	writeCache := NewLRUCache(5000, 1*time.Minute)

	return &BoltreonStore{
		db:              db,
		compressionType: compressionType,
		readCache:       readCache,
		writeCache:      writeCache,
	}, nil
}

func (s *BoltreonStore) Close() error {
	return s.db.Close()
}

// TypeOfKeyGet 用于生成存储类型的键
func TypeOfKeyGet(strKey string) []byte {
	bKey := []byte(strKey)
	bKey = append(prefixKeyTypeBytes, bKey...)
	return bKey
}

// keyBadgerGet 用于生成存储键的键
func keyBadgerGet1(bType, bKey []byte) []byte {
	bKey = append(bType, bKey...)
	return bKey
}
