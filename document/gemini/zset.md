实现Redis的zset数据结构，底层存储使用github.com/dgraph-io/badger

```go
import (
	"bytes"
	"encoding/binary"
	"math"
)

// encodeScore 对 float64 进行编码以用于排序
func encodeScore(score float64) []byte {
	bits := math.Float64bits(score)
	if score < 0 {
		bits = ^bits
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, bits)
	return b
}

// decodeScore 从字节解码回 float64
func decodeScore(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	if bits&0x8000000000000000 == 0 {
		bits = ^bits
	}
	return math.Float64frombits(bits)
}


// ZAdd 实现 ZADD 功能
func ZAdd(db *badger.DB, zsetName, member string, score float64) error {
	return db.Update(func(txn *badger.Txn) error {
		dataKey := []byte(zsetName + "|data|" + member)

		// 1. 检查旧 score
		item, err := txn.Get(dataKey)
		if err == nil { // 成员已存在
			oldScoreBytes, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			oldScore := decodeScore(oldScoreBytes)

			// 2. 删除旧的索引 Key
			oldIndexKey := []byte(zsetName + "|index|")
			oldIndexKey = append(oldIndexKey, encodeScore(oldScore)...)
			oldIndexKey = append(oldIndexKey, []byte("|" + member)...)
			if err := txn.Delete(oldIndexKey); err != nil {
				return err
			}
		} else if err != badger.ErrKeyNotFound {
			return err
		}

		// 3. 写入新数据和索引
		// 写入数据 Key
		if err := txn.Set(dataKey, encodeScore(score)); err != nil {
			return err
		}

		// 写入索引 Key
		newIndexKey := []byte(zsetName + "|index|")
		newIndexKey = append(newIndexKey, encodeScore(score)...)
		newIndexKey = append(newIndexKey, []byte("|" + member)...)
		if err := txn.Set(newIndexKey, nil); err != nil {
			return err
		}

		return nil
	})
}
type ZSetMember struct {
Member string
Score  float64
}

// ZRange 实现 ZRANGE 功能
func ZRange(db *badger.DB, zsetName string, start, stop int) ([]ZSetMember, error) {
var results []ZSetMember
err := db.View(func(txn *badger.Txn) error {
opts := badger.DefaultIteratorOptions
prefix := []byte(zsetName + "|index|")
opts.Prefix = prefix

it := txn.NewIterator(opts)
defer it.Close()

current := 0
for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
if current < start {
current++
continue
}
if stop != -1 && current > stop {
break
}

item := it.Item()
key := item.Key()

// 解析 member 和 score
parts := bytes.Split(key, []byte("|"))
member := string(parts[3])

// 从 key 中提取 score 的字节部分
scorePrefix := append(prefix, parts[2]...)
scoreBytes := key[len(prefix):len(scorePrefix)]
score := decodeScore(scoreBytes)

results = append(results, ZSetMember{Member: member, Score: score})
current++
}
return nil
})
return results, err
}
// ZRem 实现 ZREM 功能
func ZRem(db *badger.DB, zsetName, member string) error {
return db.Update(func(txn *badger.Txn) error {
dataKey := []byte(zsetName + "|data|" + member)

item, err := txn.Get(dataKey)
if err == badger.ErrKeyNotFound {
return nil // 成员不存在，无需操作
}
if err != nil {
return err
}

// 获取 score 以便删除索引
scoreBytes, err := item.ValueCopy(nil)
if err != nil {
return err
}
score := decodeScore(scoreBytes)

// 1. 删除数据 Key
if err := txn.Delete(dataKey); err != nil {
return err
}

// 2. 删除索引 Key
indexKey := []byte(zsetName + "|index|")
indexKey = append(indexKey, encodeScore(score)...)
indexKey = append(indexKey, []byte("|" + member)...)
if err := txn.Delete(indexKey); err != nil {
return err
}

return nil
})
}
```