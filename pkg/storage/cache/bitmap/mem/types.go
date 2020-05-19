package mem

import "github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"

type mem struct {
	mp map[string]*roaring.Bitmap
}
