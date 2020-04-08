package testEstimator

import (
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

func New() *testEstimator {
	return &testEstimator{}
}

func (e *testEstimator) Min(left, right op.OP) op.OP {
	if e.Less(left, right) {
		return left
	}
	return right
}

func (e *testEstimator) Less(left, right op.OP) bool {
	return left.Cost() < right.Cost()
}

func (e *testEstimator) NubSize(o op.OP, _ []string) float64 {
	return o.Size() * 0.9
}

func (e *testEstimator) FetchSize(o op.OP, _ int, _ int) float64 {
	return o.Size() * 0.7
}

func (e *testEstimator) OrderSize(o op.OP, _ []string) float64 {
	return o.Size()
}

func (e *testEstimator) SummarizeSize(_ op.OP, ops []int) float64 {
	return float64(len(ops))
}

func (e *testEstimator) RestrictSize(o op.OP, _ extend.Extend) float64 {
	return o.Size() / 3
}

func (e *testEstimator) GroupSize(o op.OP, gs []string, ops []int) float64 {
	attrs, _ := o.AttributeList()
	return float64(len(ops)) + float64(len(gs))/float64(len(attrs))*o.Size()
}

func (e *testEstimator) ProjectionSize(o op.OP, _ []string, _ []extend.Extend) float64 {
	return o.Size() * 0.8
}

func (e *testEstimator) ProductSize(r, s op.OP) float64 {
	return r.Size() * s.Size()
}

func (e *testEstimator) NaturalJoinSize(r, s op.OP) float64 {
	n, m := r.Size(), s.Size()
	if n < m {
		return m
	}
	return n
}

func (e *testEstimator) SetUnionSize(r, s op.OP) float64 {
	return (r.Size() + s.Size()) / 2.0
}

func (e *testEstimator) SetIntersectSize(r, s op.OP) float64 {
	n, m := r.Size(), s.Size()
	if n < m {
		return n / 2.0
	}
	return m / 2.0
}

func (e *testEstimator) SetDifferenceSize(r, s op.OP) float64 {
	n, m := r.Size(), s.Size()
	if n < m {
		return m - n
	}
	return n - m
}

func (e *testEstimator) MultisetUnionSize(r, s op.OP) float64 {
	return r.Size() + s.Size()
}

func (e *testEstimator) MultisetIntersectSize(r, s op.OP) float64 {
	n, m := r.Size(), s.Size()
	if n < m {
		return n / 2.0
	}
	return m / 2.0
}

func (e *testEstimator) MultisetDifferenceSize(r, s op.OP) float64 {
	n, m := r.Size(), s.Size()
	if n < m {
		return m - n
	}
	return n - m
}

func (e *testEstimator) SetUnionSizeByHash(r, s op.OP) float64 {
	return (r.Size() + s.Size()) / 2.0
}

func (e *testEstimator) SetUnionSizeByOrder(r, s op.OP) float64 {
	return (r.Size() + s.Size()) / 2.0
}

func (e *testEstimator) NubCost(o op.OP, _ []string) float64 {
	return o.Size() * 2.1
}

func (e *testEstimator) OrderCost(o op.OP, _ []string) float64 {
	return o.Cost() + o.Size()*3.3
}

func (e *testEstimator) FetchCost(o op.OP, _ int, _ int) float64 {
	return o.Cost() + o.Size()*1.0
}

func (e *testEstimator) SummarizeCost(o op.OP, ops []int) float64 {
	return o.Cost() + float64(len(ops))*1.6
}

func (e *testEstimator) GroupCost(o op.OP, gs []string, ops []int) float64 {
	return o.Cost() + float64(len(gs))*3.2 + float64(len(ops))*1.6
}

func (e *testEstimator) RestrictCost(o op.OP, _ extend.Extend) float64 {
	return o.Cost() + o.Size()*1.1
}

func (e *testEstimator) ProjectionCost(o op.OP, _ []string, _ []extend.Extend) float64 {
	return o.Cost() + o.Size()*1.5
}

func (e *testEstimator) ProductCost(r, s op.OP) float64 {
	return r.Cost() + s.Cost() + r.Size()*s.Size()
}

func (e *testEstimator) NaturalJoinCost(r, s op.OP) float64 {
	return r.Cost() + s.Cost() + r.Size() + s.Size()
}

func (e *testEstimator) SetUnionCost(r, s op.OP) float64 {
	return r.Cost() + s.Cost() + r.Size() + s.Size()
}

func (e *testEstimator) SetIntersectCost(r, s op.OP) float64 {
	return r.Cost() + s.Cost() + r.Size() + s.Size()
}

func (e *testEstimator) SetDifferenceCost(r, s op.OP) float64 {
	return r.Cost() + s.Cost() + r.Size() + s.Size()
}

func (e *testEstimator) MultisetUnionCost(r, s op.OP) float64 {
	return r.Cost() + s.Cost() + r.Size() + s.Size()
}

func (e *testEstimator) MultisetIntersectCost(r, s op.OP) float64 {
	return r.Cost() + s.Cost() + r.Size() + s.Size()
}

func (e *testEstimator) MultisetDifferenceCost(r, s op.OP) float64 {
	return r.Cost() + s.Cost() + r.Size() + s.Size()
}

func (e *testEstimator) SetUnionCostByHash(r, s op.OP) float64 {
	return r.Cost() + s.Cost() + (r.Size()+s.Size())*0.55
}

func (e *testEstimator) SetUnionCostByOrder(r, s op.OP) float64 {
	return r.Cost() + s.Cost() + (r.Size()+s.Size())*0.7
}
