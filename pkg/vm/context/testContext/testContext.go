package testContext

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/counter"
	cmem "github.com/deepfabric/thinkbase/pkg/vm/container/counter/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictVec"
	dvmem "github.com/deepfabric/thinkbase/pkg/vm/container/dictVec/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictionary"
	dmem "github.com/deepfabric/thinkbase/pkg/vm/container/dictionary/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/container/hash"
	hmem "github.com/deepfabric/thinkbase/pkg/vm/container/hash/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/container/vector"
	vmem "github.com/deepfabric/thinkbase/pkg/vm/container/vector/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/estimator/testEstimator"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/workspace/testWorkspace"
)

func New(mcpu, rcpu, memSize, diskSize int) *testContext {
	return &testContext{mcpu, rcpu, memSize, diskSize, testEstimator.New(), testWorkspace.New()}
}

func (c *testContext) Min(left, right op.OP) op.OP {
	return c.est.Min(left, right)
}

func (c *testContext) Less(left, right op.OP) bool {
	return c.est.Less(left, right)
}

func (c *testContext) NubSize(o op.OP, attrs []string) float64 {
	return c.est.NubSize(o, attrs)
}

func (c *testContext) FetchSize(o op.OP, limit, offset int) float64 {
	return c.est.FetchSize(o, limit, offset)
}

func (c *testContext) OrderSize(o op.OP, attrs []string) float64 {
	return c.est.OrderSize(o, attrs)
}

func (c *testContext) SummarizeSize(o op.OP, ops []int) float64 {
	return c.est.SummarizeSize(o, ops)
}

func (c *testContext) GroupSize(o op.OP, gs []string, ops []int) float64 {
	return c.est.GroupSize(o, gs, ops)
}

func (c *testContext) RestrictSize(o op.OP, e extend.Extend) float64 {
	return c.est.RestrictSize(o, e)
}

func (c *testContext) ProjectionSize(o op.OP, as []string, es []extend.Extend) float64 {
	return c.est.ProjectionSize(o, as, es)
}

func (c *testContext) ProductSize(r, s op.OP) float64 {
	return c.est.ProductSize(r, s)
}

func (c *testContext) NaturalJoinSize(r, s op.OP) float64 {
	return c.est.NaturalJoinSize(r, s)
}

func (c *testContext) SetUnionSize(r, s op.OP) float64 {
	return c.est.SetUnionSize(r, s)
}

func (c *testContext) SetIntersectSize(r, s op.OP) float64 {
	return c.est.SetIntersectSize(r, s)
}

func (c *testContext) SetDifferenceSize(r, s op.OP) float64 {
	return c.est.SetDifferenceSize(r, s)
}

func (c *testContext) MultisetUnionSize(r, s op.OP) float64 {
	return c.est.MultisetUnionSize(r, s)
}

func (c *testContext) MultisetIntersectSize(r, s op.OP) float64 {
	return c.est.MultisetIntersectSize(r, s)
}

func (c *testContext) MultisetDifferenceSize(r, s op.OP) float64 {
	return c.est.MultisetDifferenceSize(r, s)
}

func (c *testContext) SetUnionSizeByHash(r, s op.OP) float64 {
	return c.est.SetUnionSizeByHash(r, s)
}

func (c *testContext) SetUnionSizeByOrder(r, s op.OP) float64 {
	return c.est.SetUnionSizeByOrder(r, s)
}

func (c *testContext) NubCost(o op.OP, attrs []string) float64 {
	return c.est.NubCost(o, attrs)
}

func (c *testContext) FetchCost(o op.OP, limit, offset int) float64 {
	return c.est.FetchCost(o, limit, offset)
}

func (c *testContext) OrderCost(o op.OP, attrs []string) float64 {
	return c.est.OrderCost(o, attrs)
}

func (c *testContext) SummarizeCost(o op.OP, ops []int) float64 {
	return c.est.SummarizeCost(o, ops)
}

func (c *testContext) GroupCost(o op.OP, gs []string, ops []int) float64 {
	return c.est.GroupCost(o, gs, ops)
}

func (c *testContext) RestrictCost(o op.OP, e extend.Extend) float64 {
	return c.est.RestrictCost(o, e)
}

func (c *testContext) ProjectionCost(o op.OP, as []string, es []extend.Extend) float64 {
	return c.est.ProjectionCost(o, as, es)
}

func (c *testContext) ProductCost(r, s op.OP) float64 {
	return c.est.ProductCost(r, s)
}

func (c *testContext) NaturalJoinCost(r, s op.OP) float64 {
	return c.est.NaturalJoinCost(r, s)
}

func (c *testContext) SetUnionCost(r, s op.OP) float64 {
	return c.est.SetUnionCost(r, s)
}

func (c *testContext) SetIntersectCost(r, s op.OP) float64 {
	return c.est.SetIntersectCost(r, s)
}

func (c *testContext) SetDifferenceCost(r, s op.OP) float64 {
	return c.est.SetDifferenceCost(r, s)
}

func (c *testContext) MultisetUnionCost(r, s op.OP) float64 {
	return c.est.MultisetUnionCost(r, s)
}

func (c *testContext) MultisetIntersectCost(r, s op.OP) float64 {
	return c.est.MultisetIntersectSize(r, s)
}

func (c *testContext) MultisetDifferenceCost(r, s op.OP) float64 {
	return c.est.MultisetDifferenceCost(r, s)
}

func (c *testContext) SetUnionCostByHash(r, s op.OP) float64 {
	return c.est.SetUnionCostByHash(r, s)
}

func (c *testContext) SetUnionCostByOrder(r, s op.OP) float64 {
	return c.est.SetUnionCostByOrder(r, s)
}

func (c *testContext) Id() string {
	return c.wsp.Id()
}

func (c *testContext) Database() string {
	return c.wsp.Database()
}

func (c *testContext) Relation(name string) (relation.Relation, error) {
	return c.wsp.Relation(name)
}

func (c *testContext) NumMcpu() int {
	return c.mcpu
}

func (c *testContext) NumRcpu() int {
	return c.rcpu
}

func (c *testContext) MemSize() int {
	return c.memSize
}

func (c *testContext) DiskSize() int {
	return c.diskSize
}

func (c *testContext) NewHash(n int) (hash.Hash, error) {
	return hmem.New(n, c.NewVector), nil
}

func (c *testContext) NewVector() (vector.Vector, error) {
	return vmem.New(), nil
}

func (c *testContext) NewCounter() (counter.Counter, error) {
	return cmem.New(), nil
}

func (c *testContext) NewDictVector() (dictVec.DictVector, error) {
	return dvmem.New(), nil
}

func (c *testContext) NewDictionary() (dictionary.Dictionary, error) {
	return dmem.New(), nil
}
