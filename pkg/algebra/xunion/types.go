package xunion

import "github.com/deepfabric/thinkbase/pkg/algebra/relation"

type Xunion interface {
	Xunion() relation.Relation
}

type xunion struct {
	a, b relation.Relation
}
