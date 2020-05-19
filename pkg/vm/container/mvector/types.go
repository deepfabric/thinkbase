package mvector

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"
)

type Mvector interface {
	Destroy() error

	Get(int) (*roaring.Bitmap, error)

	Append([]*roaring.Bitmap) error
}

type mvector struct {
	sync.RWMutex
	ms []*roaring.Bitmap
}
