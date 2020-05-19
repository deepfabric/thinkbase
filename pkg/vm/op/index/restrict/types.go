package restrict

import (
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/index/filter"

	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
)

type RestrictOP interface {
	op.OP
	Filter() filter.Filter
}

type restrict struct {
	isCheck bool
	row     uint64
	rows    uint64
	is      []uint64
	fl      filter.Filter
	c       context.Context
	r       relation.Relation
}
