package store

// BadgerStore 保持向后兼容的别名
type BadgerStore = BoltreonStore

// NewBadgerStore 与历史用法兼容的构造函数
func NewBadgerStore(path string) (*BadgerStore, error) {
	return NewBoltreonStore(path)
}
