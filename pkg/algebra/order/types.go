package order

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type Order interface {
	Order() (relation.Relation, error)
}

type order struct {
	isNub bool
	descs []bool
	attrs []string
	c     context.Context
	r     relation.Relation
}
