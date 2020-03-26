package hash

import "hash/crc32"

func GenHash(data []byte) int {
	return int(crc32.ChecksumIEEE(data))
}
