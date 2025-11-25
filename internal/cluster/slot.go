package cluster

import "strings"

const SlotCount = 16384

func Slot(key string) uint32 {
	// Redis 官方 CRC16 + {} 规则
	start := strings.IndexByte(key, '{')
	if start == -1 {
		return crc16([]byte(key)) % SlotCount
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end == -1 {
		return crc16([]byte(key)) % SlotCount
	}
	return crc16([]byte(key[start+1:start+1+end])) % SlotCount
}
