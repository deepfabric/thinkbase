package srangebitmap

import "github.com/deepfabric/thinkbase/pkg/storage/ranging/textranging"

type Cache interface {
	Set(string, *textranging.Ranging)
	Get(string) (*textranging.Ranging, bool)
}
