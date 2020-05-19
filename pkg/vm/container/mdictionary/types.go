package mdictionary

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"
)

type Mdictionary interface {
	Destroy() error

	Set(string, *roaring.Bitmap) error
	Range(func(string, *roaring.Bitmap))
}

type mdictionary struct {
	sync.RWMutex
	mp map[string]*roaring.Bitmap
}
