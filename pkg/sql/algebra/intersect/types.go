package intersect

import "github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"

type Intersect interface {
	Intersect() relation.Relation
}

type intersect struct {
	isNub bool
	a, b  relation.Relation
}
