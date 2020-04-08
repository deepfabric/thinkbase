package estimator

import (
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

type Estimator interface {
	Less(op.OP, op.OP) bool

	Min(op.OP, op.OP) op.OP

	NubSize(op.OP, []string) float64
	FetchSize(op.OP, int, int) float64
	OrderSize(op.OP, []string) float64
	SummarizeSize(op.OP, []int) float64
	GroupSize(op.OP, []string, []int) float64
	RestrictSize(op.OP, extend.Extend) float64
	ProjectionSize(op.OP, []string, []extend.Extend) float64

	ProductSize(op.OP, op.OP) float64

	NaturalJoinSize(op.OP, op.OP) float64

	SetUnionSize(op.OP, op.OP) float64
	SetIntersectSize(op.OP, op.OP) float64
	SetDifferenceSize(op.OP, op.OP) float64

	MultisetUnionSize(op.OP, op.OP) float64
	MultisetIntersectSize(op.OP, op.OP) float64
	MultisetDifferenceSize(op.OP, op.OP) float64

	SetUnionSizeByHash(op.OP, op.OP) float64

	SetUnionSizeByOrder(op.OP, op.OP) float64

	NubCost(op.OP, []string) float64
	FetchCost(op.OP, int, int) float64
	OrderCost(op.OP, []string) float64
	SummarizeCost(op.OP, []int) float64
	GroupCost(op.OP, []string, []int) float64
	RestrictCost(op.OP, extend.Extend) float64
	ProjectionCost(op.OP, []string, []extend.Extend) float64

	ProductCost(op.OP, op.OP) float64

	NaturalJoinCost(op.OP, op.OP) float64

	SetUnionCost(op.OP, op.OP) float64
	SetIntersectCost(op.OP, op.OP) float64
	SetDifferenceCost(op.OP, op.OP) float64

	MultisetUnionCost(op.OP, op.OP) float64
	MultisetIntersectCost(op.OP, op.OP) float64
	MultisetDifferenceCost(op.OP, op.OP) float64

	SetUnionCostByHash(op.OP, op.OP) float64

	SetUnionCostByOrder(op.OP, op.OP) float64
}
