package testunit

import (
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/projection"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

type unionUnit struct {
	a relation.Relation
}

type minusUnit struct {
	a, b relation.Relation
}

type intersectUnit struct {
	a, b relation.Relation
}

type restrictUnit struct {
	e extend.Extend
	r relation.Relation
}

type projectionUnit struct {
	r  relation.Relation
	as []*projection.Attribute
}
