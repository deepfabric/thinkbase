package rangebitmap

import "github.com/deepfabric/thinkbase/pkg/storage/ranging"

type Cache interface {
	Set(string, *ranging.Ranging)
	Get(string) (*ranging.Ranging, bool)
}
