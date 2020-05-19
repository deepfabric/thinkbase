package estimator

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/index/filter"
)

func New() *estimator {
	return &estimator{}
}

func (e *estimator) NubSize(_ op.OP, _ []string) float64 {
	return 0.0
}

func (e *estimator) FetchSize(_ op.OP, _ int, _ int) float64 {
	return 0.0
}

func (e *estimator) OrderSize(_ op.OP, _ []string) float64 {
	return 0.0
}

func (e *estimator) SummarizeSize(_ op.OP, _ []int) float64 {
	return 0.0
}

func (e *estimator) RestrictSize(o op.OP, _ extend.Extend) float64 {
	return 0.0
}

func (e *estimator) GroupSize(_ op.OP, _ []string, _ []int) float64 {
	return 0.0
}

func (e *estimator) ProjectionSize(_ op.OP, _ []string, _ []extend.Extend) float64 {
	return 0.0
}

func (e *estimator) SummarizeSizeWithIndex(_ op.OP, _ []int) float64 {
	return 0.0
}

func (e *estimator) GroupSizeWithIndex(_ op.OP, _ []string, _ []int) float64 {
	return 0.0
}

func (e *estimator) RestrictSizeWithIndex(_ relation.Relation, _ filter.Filter) float64 {
	return 0.0
}

func (e *estimator) NubCost(_ op.OP, _ []string) float64 {
	return 0.0
}

func (e *estimator) OrderCost(_ op.OP, _ []string) float64 {
	return 0.0
}

func (e *estimator) FetchCost(_ op.OP, _ int, _ int) float64 {
	return 0.0
}

func (e *estimator) SummarizeCost(_ op.OP, _ []int) float64 {
	return 0.0
}

func (e *estimator) GroupCost(_ op.OP, _ []string, _ []int) float64 {
	return 0.0
}

func (e *estimator) RestrictCost(_ op.OP, _ extend.Extend) float64 {
	return 0.0
}

func (e *estimator) ProjectionCost(_ op.OP, _ []string, _ []extend.Extend) float64 {
	return 0.0
}

func (e *estimator) SummarizeCostWithIndex(_ op.OP, _ []int) float64 {
	return 0.0
}

func (e *estimator) GroupCostWithIndex(_ op.OP, _ []string, _ []int) float64 {
	return 0.0
}

func (e *estimator) RestrictCostWithIndex(_ relation.Relation, _ filter.Filter) float64 {
	return 0.0
}

func (e *estimator) AttributeCount(_ relation.Relation, _ string, typ int) int {
	return 0
}
