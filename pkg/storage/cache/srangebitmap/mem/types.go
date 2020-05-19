package mem

import "github.com/deepfabric/thinkbase/pkg/storage/ranging/textranging"

type mem struct {
	mp map[string]*textranging.Ranging
}
