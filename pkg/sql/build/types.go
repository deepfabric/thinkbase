package build

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize"
)

const (
	On = iota
	Where
	Having
	Projection
)

type table struct {
	isAlias bool
	o       op.OP
	name    string
	attrs   []string
	r       relation.Relation
}

type tables struct {
	ts []*table
}

type summarizeOp struct {
	mp map[string]struct{}
	es []*summarize.Extend
}

type build struct {
	sql string
	ts  []*tables
	ss  *summarizeOp
	c   context.Context
	mp  map[string]struct{}
}
