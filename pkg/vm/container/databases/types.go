package databases

import "github.com/deepfabric/thinkbase/pkg/vm/container/database"

type Databases interface {
	Destroy() error

	Database(string) (database.Database, error)
}
