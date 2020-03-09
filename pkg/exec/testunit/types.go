package testunit

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/projection"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/summarize"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type orderUnit struct {
	isNub bool
	descs []bool
	attrs []string
	c     context.Context
	r     relation.Relation
}

type unionUnit struct {
	c context.Context
	a relation.Relation
}

type minusUnit struct {
	c    context.Context
	a, b relation.Relation
}

type productUnit struct {
	c    context.Context
	a, b relation.Relation
}

type summarizeUnit struct {
	ops   []int
	gs    []string
	c     context.Context
	r     relation.Relation
	attrs []*summarize.Attribute
}

type intersectUnit struct {
	c    context.Context
	a, b relation.Relation
}

type restrictUnit struct {
	plh int
	e   extend.Extend
	c   context.Context
	r   relation.Relation
}

type projectionUnit struct {
	plh int
	c   context.Context
	r   relation.Relation
	as  []*projection.Attribute
}
