package intersect

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type Intersect interface {
	Intersect() relation.Relation
}

type intersect struct {
	c    context.Context
	a, b relation.Relation
}
