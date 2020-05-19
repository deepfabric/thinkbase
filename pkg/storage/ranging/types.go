package ranging

import "github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"

type Ranging struct {
	mp *roaring.Bitmap
}

const (
	Int = iota
	Time
	Float
)

const (
	bsiExistsBit = 0
	bsiSignBit   = 1
	bsiOffsetBit = 2

	ShardWidth = 1 << 20

	Scale = 10000
)
