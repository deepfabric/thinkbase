package xunion

import "github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"

type Xunion interface {
	Xunion() relation.Relation
}

type xunion struct {
	isNub bool
	a, b  relation.Relation
}
