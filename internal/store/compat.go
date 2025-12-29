package store

// BadgerStore 保持向后兼容的别名
type BadgerStore = BoltreonStore

// NewBadgerStore 与历史用法兼容的构造函数（使用默认LZ4压缩）
func NewBadgerStore(path string) (*BadgerStore, error) {
	return NewBoltreonStore(path)
}

// NewBadgerStoreWithCompression 创建BadgerStore实例，指定压缩算法
func NewBadgerStoreWithCompression(path string, compressionType CompressionType) (*BadgerStore, error) {
	return NewBoltreonStoreWithCompression(path, compressionType)
}

// SetCompression 设置压缩算法（运行时修改）
func (s *BoltreonStore) SetCompression(compressionType CompressionType) {
	s.compressionType = compressionType
}

// GetCompression 获取当前压缩算法
func (s *BoltreonStore) GetCompression() CompressionType {
	return s.compressionType
}
