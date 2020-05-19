package bitmap

import "github.com/deepfabric/thinkbase/pkg/storage/ranging/roaring"

type Cache interface {
	Set(string, *roaring.Bitmap)
	Get(string) (*roaring.Bitmap, bool)
}
