package cluster

import "strings"

const SlotCount = 16384

// crc16 implements CRC-16/XModem (polynomial 0x1021, initial value 0x0000)
// This matches Redis's CRC16 implementation for slot calculation
func crc16(data []byte) uint16 {
	crc := uint16(0)
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

func Slot(key string) uint32 {
	// Redis 官方 CRC16 + {} 规则
	start := strings.IndexByte(key, '{')
	if start == -1 {
		return uint32(crc16([]byte(key))) % SlotCount
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end == -1 {
		return uint32(crc16([]byte(key))) % SlotCount
	}
	return uint32(crc16([]byte(key[start+1:start+1+end]))) % SlotCount
}
