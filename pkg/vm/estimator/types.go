package estimator

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/index/filter"
)

type Estimator interface {
	NubSize(op.OP, []string) float64
	FetchSize(op.OP, int, int) float64
	OrderSize(op.OP, []string) float64
	SummarizeSize(op.OP, []int) float64
	GroupSize(op.OP, []string, []int) float64
	RestrictSize(op.OP, extend.Extend) float64
	ProjectionSize(op.OP, []string, []extend.Extend) float64

	SummarizeSizeWithIndex(op.OP, []int) float64
	GroupSizeWithIndex(op.OP, []string, []int) float64
	RestrictSizeWithIndex(relation.Relation, filter.Filter) float64

	NubCost(op.OP, []string) float64
	FetchCost(op.OP, int, int) float64
	OrderCost(op.OP, []string) float64
	SummarizeCost(op.OP, []int) float64
	GroupCost(op.OP, []string, []int) float64
	RestrictCost(op.OP, extend.Extend) float64
	ProjectionCost(op.OP, []string, []extend.Extend) float64

	SummarizeCostWithIndex(op.OP, []int) float64
	GroupCostWithIndex(op.OP, []string, []int) float64
	RestrictCostWithIndex(relation.Relation, filter.Filter) float64

	AttributeCount(relation.Relation, string, int) int
}

type estimator struct {
}
