package group

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictVector"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize"
)

type GroupOP interface {
	op.OP
	Group() []string
	Extend() extend.Extend
	Extends() []*summarize.Extend
}

// 没有聚合的分组会被转换为nub(projection(r, gs), gs)
type group struct {
	isCheck bool
	prev    op.OP
	k       string
	gs      []string // group attributes
	e       extend.Extend
	c       context.Context
	es      []*summarize.Extend
	dv      dictVector.DictVector
}
