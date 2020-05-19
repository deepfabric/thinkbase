package mem

import "github.com/deepfabric/thinkbase/pkg/storage/ranging"

type mem struct {
	mp map[string]*ranging.Ranging
}
