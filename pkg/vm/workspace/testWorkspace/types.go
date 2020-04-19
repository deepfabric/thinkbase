package testWorkspace

import "github.com/deepfabric/thinkbase/pkg/vm/container/relation"

type testWorkspace struct {
	id string
	db string
	mp map[string]relation.Relation
}
