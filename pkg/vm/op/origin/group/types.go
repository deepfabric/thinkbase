package group

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictVec"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize"
)

// 没有聚合的分组会被转换为nub(projection(r, gs), gs)
type group struct {
	isCheck bool
	prev    op.OP
	k       string
	gs      []string // group attributes
	e       extend.Extend
	c       context.Context
	dv      dictVec.DictVector
	es      []*summarize.Extend
}
