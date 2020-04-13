package workspace

import "github.com/deepfabric/thinkbase/pkg/vm/container/databases"

type Workspace interface {
	Destroy() error

	Databases(string) (databases.Databases, error)
}
