package context

import (
	"fmt"
	"time"

	"github.com/deepfabric/thinkbase/pkg/vm/container/dictVector"
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictionary"
	"github.com/deepfabric/thinkbase/pkg/vm/container/mdictionary"
	"github.com/deepfabric/thinkbase/pkg/vm/container/mvector"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/estimator"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/index/filter"
	"github.com/deepfabric/thinkbase/pkg/vm/workspace"
)

func New(cfg *Config, est estimator.Estimator, wsp workspace.Workspace) *context {
	return &context{
		est:       est,
		wsp:       wsp,
		uid:       cfg.Uid,
		mcpu:      cfg.Mcpu,
		rcpu:      cfg.Rcpu,
		memSize:   cfg.MemSize,
		diskSize:  cfg.DiskSize,
		blockSize: cfg.BlockSize,
	}
}

func NewConfig(uid string) *Config {
	return &Config{
		Mcpu:      1,
		Rcpu:      1,
		Uid:       uid,
		BlockSize: 16 * 1024 * 1024,
		MemSize:   64 * 1024 * 1024,
		DiskSize:  64 * 1024 * 1024 * 1024,
	}
}

func (c *context) NubSize(o op.OP, attrs []string) float64 {
	return c.est.NubSize(o, attrs)
}

func (c *context) FetchSize(o op.OP, limit, offset int) float64 {
	return c.est.FetchSize(o, limit, offset)
}

func (c *context) OrderSize(o op.OP, attrs []string) float64 {
	return c.est.OrderSize(o, attrs)
}

func (c *context) SummarizeSize(o op.OP, ops []int) float64 {
	return c.est.SummarizeSize(o, ops)
}

func (c *context) GroupSize(o op.OP, gs []string, ops []int) float64 {
	return c.est.GroupSize(o, gs, ops)
}

func (c *context) RestrictSize(o op.OP, e extend.Extend) float64 {
	return c.est.RestrictSize(o, e)
}

func (c *context) ProjectionSize(o op.OP, as []string, es []extend.Extend) float64 {
	return c.est.ProjectionSize(o, as, es)
}

func (c *context) SummarizeSizeWithIndex(o op.OP, ops []int) float64 {
	return c.est.SummarizeSizeWithIndex(o, ops)
}

func (c *context) GroupSizeWithIndex(o op.OP, gs []string, ops []int) float64 {
	return c.est.GroupSizeWithIndex(o, gs, ops)
}

func (c *context) RestrictSizeWithIndex(r relation.Relation, fl filter.Filter) float64 {
	return c.est.RestrictSizeWithIndex(r, fl)
}

func (c *context) NubCost(o op.OP, attrs []string) float64 {
	return c.est.NubCost(o, attrs)
}

func (c *context) FetchCost(o op.OP, limit, offset int) float64 {
	return c.est.FetchCost(o, limit, offset)
}

func (c *context) OrderCost(o op.OP, attrs []string) float64 {
	return c.est.OrderCost(o, attrs)
}

func (c *context) SummarizeCost(o op.OP, ops []int) float64 {
	return c.est.SummarizeCost(o, ops)
}

func (c *context) GroupCost(o op.OP, gs []string, ops []int) float64 {
	return c.est.GroupCost(o, gs, ops)
}

func (c *context) RestrictCost(o op.OP, e extend.Extend) float64 {
	return c.est.RestrictCost(o, e)
}

func (c *context) ProjectionCost(o op.OP, as []string, es []extend.Extend) float64 {
	return c.est.ProjectionCost(o, as, es)
}

func (c *context) SummarizeCostWithIndex(o op.OP, ops []int) float64 {
	return c.est.SummarizeCostWithIndex(o, ops)
}

func (c *context) GroupCostWithIndex(o op.OP, gs []string, ops []int) float64 {
	return c.est.GroupCostWithIndex(o, gs, ops)
}

func (c *context) RestrictCostWithIndex(r relation.Relation, fl filter.Filter) float64 {
	return c.est.RestrictCostWithIndex(r, fl)
}

func (c *context) AttributeCount(r relation.Relation, attr string, typ int) int {
	return c.est.AttributeCount(r, attr, typ)
}

func (c *context) Id() string {
	return c.wsp.Id()
}

func (c *context) Database() string {
	return c.wsp.Database()
}

func (c *context) Relation(name string) (relation.Relation, error) {
	return c.wsp.Relation(name)
}

func (c *context) NumMcpu() int {
	return c.mcpu
}

func (c *context) NumRcpu() int {
	return c.rcpu
}

func (c *context) MemSize() int {
	return c.memSize
}

func (c *context) DiskSize() int {
	return c.diskSize
}

func (c *context) BlockSize() int {
	return c.blockSize
}

func (c *context) NewDictionary() (dictionary.Dictionary, error) {
	return dictionary.New(fmt.Sprintf("%s.%v.d", c.uid, time.Now().UnixNano()), c.memSize), nil
}

func (c *context) NewDictVector() (dictVector.DictVector, error) {
	return dictVector.New(fmt.Sprintf("%s.%v.dv", c.uid, time.Now().UnixNano()), c.memSize), nil
}

func (c *context) NewMvector() (mvector.Mvector, error) {
	return mvector.New(), nil
}

func (c *context) NewMdictionary() (mdictionary.Mdictionary, error) {
	return mdictionary.New(), nil
}
