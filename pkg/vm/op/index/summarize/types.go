package summarize

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op/index/filter"
)

type Extend struct {
	Op    int
	Typ   int
	Name  string
	Alias string
}

type summarize struct {
	isUsed  bool
	isCheck bool
	rows    uint64
	es      []*Extend
	fl      filter.Filter
	c       context.Context
	r       relation.Relation
}
