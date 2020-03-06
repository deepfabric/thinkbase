package testunit

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/projection"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/summarize"
)

type orderUnit struct {
	isNub bool
	descs []bool
	attrs []string
	r     relation.Relation
}

type unionUnit struct {
	a relation.Relation
}

type minusUnit struct {
	a, b relation.Relation
}

type productUnit struct {
	a, b relation.Relation
}

type summarizeUnit struct {
	ops   []int
	gs    []string
	r     relation.Relation
	attrs []*summarize.Attribute
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
