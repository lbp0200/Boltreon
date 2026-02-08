package replication

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/Botreon/internal/logger"
	"github.com/lbp0200/Botreon/internal/store"
)

const (
	RDBMagicString = "REDIS"
	RDBVersion     = "0009" // Redis RDB version 9
)

// RDBEncoder RDB编码器
type RDBEncoder struct {
	buf *bytes.Buffer
}

// NewRDBEncoder 创建新的RDB编码器
func NewRDBEncoder() *RDBEncoder {
	enc := &RDBEncoder{
		buf: &bytes.Buffer{},
	}
	enc.writeHeader()
	return enc
}

// writeHeader 写入RDB文件头
func (enc *RDBEncoder) writeHeader() {
	enc.buf.WriteString(RDBMagicString)
	enc.buf.WriteString(RDBVersion)
}

// WriteDatabaseSelector 写入数据库选择器
func (enc *RDBEncoder) WriteDatabaseSelector(dbNum int) {
	enc.buf.WriteByte(0xFE) // FE = database selector
	enc.writeLength(uint64(dbNum))
}

// WriteKeyValue 写入键值对
func (enc *RDBEncoder) WriteKeyValue(key string, value interface{}, keyType string, ttl int64) error {
	// 如果有TTL，写入过期时间
	if ttl > 0 {
		now := time.Now().Unix()
		expireTime := now + ttl
		enc.buf.WriteByte(0xFD) // FD = expire time in seconds
		binary.Write(enc.buf, binary.LittleEndian, uint32(expireTime))
	}

	// 写入值类型
	var typeByte byte
	switch keyType {
	case store.KeyTypeString:
		typeByte = 0 // STRING
	case store.KeyTypeList:
		typeByte = 1 // LIST
	case store.KeyTypeSet:
		typeByte = 2 // SET
	case store.KeyTypeHash:
		typeByte = 3 // HASH
	case store.KeyTypeSortedSet:
		typeByte = 4 // ZSET
	default:
		return fmt.Errorf("unknown key type: %s", keyType)
	}

	enc.buf.WriteByte(typeByte)

	// 写入键
	enc.writeString(key)

	// 写入值
	switch v := value.(type) {
	case string:
		enc.writeString(v)
	case []string:
		// List
		enc.writeLength(uint64(len(v)))
		for _, item := range v {
			enc.writeString(item)
		}
	case map[string][]byte:
		// Hash
		enc.writeLength(uint64(len(v)))
		for field, val := range v {
			enc.writeString(field)
			enc.writeBytes(val)
		}
	default:
		return fmt.Errorf("unsupported value type")
	}

	return nil
}

// WriteStringKeyValue 写入字符串键值对
func (enc *RDBEncoder) WriteStringKeyValue(key, value string, ttl int64) error {
	if ttl > 0 {
		now := time.Now().Unix()
		expireTime := now + ttl
		enc.buf.WriteByte(0xFD)
		binary.Write(enc.buf, binary.LittleEndian, uint32(expireTime))
	}

	enc.buf.WriteByte(0) // STRING type
	enc.writeString(key)
	enc.writeString(value)
	return nil
}

// WriteListKeyValue 写入列表键值对
func (enc *RDBEncoder) WriteListKeyValue(key string, values []string, ttl int64) error {
	if ttl > 0 {
		now := time.Now().Unix()
		expireTime := now + ttl
		enc.buf.WriteByte(0xFD)
		binary.Write(enc.buf, binary.LittleEndian, uint32(expireTime))
	}

	enc.buf.WriteByte(1) // LIST type
	enc.writeString(key)
	enc.writeLength(uint64(len(values)))
	for _, v := range values {
		enc.writeString(v)
	}
	return nil
}

// WriteHashKeyValue 写入哈希键值对
func (enc *RDBEncoder) WriteHashKeyValue(key string, fields map[string][]byte, ttl int64) error {
	if ttl > 0 {
		now := time.Now().Unix()
		expireTime := now + ttl
		enc.buf.WriteByte(0xFD)
		binary.Write(enc.buf, binary.LittleEndian, uint32(expireTime))
	}

	enc.buf.WriteByte(3) // HASH type
	enc.writeString(key)
	enc.writeLength(uint64(len(fields)))
	for field, value := range fields {
		enc.writeString(field)
		enc.writeBytes(value)
	}
	return nil
}

// WriteSetKeyValue 写入集合键值对
func (enc *RDBEncoder) WriteSetKeyValue(key string, members []string, ttl int64) error {
	if ttl > 0 {
		now := time.Now().Unix()
		expireTime := now + ttl
		enc.buf.WriteByte(0xFD)
		binary.Write(enc.buf, binary.LittleEndian, uint32(expireTime))
	}

	enc.buf.WriteByte(2) // SET type
	enc.writeString(key)
	enc.writeLength(uint64(len(members)))
	for _, member := range members {
		enc.writeString(member)
	}
	return nil
}

// WriteFooter 写入RDB文件尾
func (enc *RDBEncoder) WriteFooter() {
	enc.buf.WriteByte(0xFF) // FF = end of RDB file
	// 校验和（简化实现，实际应该计算CRC64）
	enc.buf.Write(make([]byte, 8))
}

// Bytes 获取编码后的字节
func (enc *RDBEncoder) Bytes() []byte {
	return enc.buf.Bytes()
}

// WriteTo 写入到Writer
func (enc *RDBEncoder) WriteTo(w io.Writer) (int64, error) {
	n, err := enc.buf.WriteTo(w)
	return n, err
}

// writeString 写入字符串（长度编码）
func (enc *RDBEncoder) writeString(s string) {
	enc.writeLength(uint64(len(s)))
	enc.buf.WriteString(s)
}

// writeBytes 写入字节数组
func (enc *RDBEncoder) writeBytes(b []byte) {
	enc.writeLength(uint64(len(b)))
	enc.buf.Write(b)
}

// writeLength 写入长度（使用Redis长度编码）
func (enc *RDBEncoder) writeLength(length uint64) {
	if length < 0x40 {
		// 6位长度
		enc.buf.WriteByte(byte(length))
	} else if length < 0x4000 {
		// 14位长度
		enc.buf.WriteByte(byte((length >> 8) | 0x40))
		enc.buf.WriteByte(byte(length & 0xFF))
	} else {
		// 32位长度
		enc.buf.WriteByte(0x80)
		binary.Write(enc.buf, binary.LittleEndian, uint32(length))
	}
}

// GenerateRDB 生成RDB快照
func GenerateRDB(s *store.BotreonStore) ([]byte, error) {
	enc := NewRDBEncoder()

	// 选择数据库0
	enc.WriteDatabaseSelector(0)

	// 遍历所有键
	err := s.GetDB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		iter := txn.NewIterator(opts)
		defer iter.Close()

		prefix := []byte("TYPE_")
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			item := iter.Item()
			keyBytes := item.KeyCopy(nil)
			key := string(keyBytes[len(prefix):])

			// 获取键类型
			typeVal, err := item.ValueCopy(nil)
			if err != nil {
				logger.Logger.Warn().Str("key", key).Err(err).Msg("获取键类型失败")
				continue
			}
			keyType := string(typeVal)

			// 获取TTL
			ttl := int64(0)
			if item.ExpiresAt() > 0 {
				expireTime := time.Unix(int64(item.ExpiresAt()), 0)
				now := time.Now()
				if expireTime.After(now) {
					ttl = int64(expireTime.Sub(now).Seconds())
				}
			}

			// 根据类型获取值并写入
			switch keyType {
			case store.KeyTypeString:
				value, err := s.Get(key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取字符串值失败")
					continue
				}
				enc.WriteStringKeyValue(key, value, ttl)

			case store.KeyTypeList:
				values, err := s.LRange(key, 0, -1)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取列表值失败")
					continue
				}
				enc.WriteListKeyValue(key, values, ttl)

			case store.KeyTypeHash:
				fields, err := s.HGetAll(key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取哈希值失败")
					continue
				}
				enc.WriteHashKeyValue(key, fields, ttl)

			case store.KeyTypeSet:
				members, err := s.SMembers(key)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取集合值失败")
					continue
				}
				enc.WriteSetKeyValue(key, members, ttl)

			case store.KeyTypeSortedSet:
				// SortedSet需要特殊处理
				members, err := s.ZRange(key, 0, -1)
				if err != nil {
					logger.Logger.Warn().Str("key", key).Err(err).Msg("获取有序集合值失败")
					continue
				}
				// 写入SortedSet（简化实现）
				if ttl > 0 {
					now := time.Now().Unix()
					expireTime := now + ttl
					enc.buf.WriteByte(0xFD)
					binary.Write(enc.buf, binary.LittleEndian, uint32(expireTime))
				}
				enc.buf.WriteByte(4) // ZSET type
				enc.writeString(key)
				enc.writeLength(uint64(len(members)))
				for _, m := range members {
					enc.writeString(m.Member)
					scoreBytes := []byte(fmt.Sprintf("%.10g", m.Score))
					enc.writeBytes(scoreBytes)
				}

			default:
				logger.Logger.Warn().Str("key", key).Str("type", keyType).Msg("未知键类型")
				continue
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("生成RDB失败: %w", err)
	}

	// 写入文件尾
	enc.WriteFooter()

	return enc.Bytes(), nil
}

