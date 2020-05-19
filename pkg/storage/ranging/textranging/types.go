package textranging

import "github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"

type Ranging struct {
	mp *roaring.Bitmap
}

const (
	ShardWidth = 1 << 20
)
