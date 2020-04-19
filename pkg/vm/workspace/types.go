package workspace

import "github.com/deepfabric/thinkbase/pkg/vm/container/relation"

type Workspace interface {
	Id() string
	Database() string
	Relation(string) (relation.Relation, error)
}
