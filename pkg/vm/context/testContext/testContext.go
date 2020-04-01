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
	"github.com/deepfabric/thinkbase/pkg/vm/container/vector"
	vmem "github.com/deepfabric/thinkbase/pkg/vm/container/vector/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/estimator/testEstimator"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

func New(mcpu, rcpu, memSize, diskSize int) *testContext {
	return &testContext{mcpu, rcpu, memSize, diskSize, testEstimator.New()}
}

func (c *testContext) Min(left, right op.OP) op.OP {
	return c.est.Min(left, right)
}

func (c *testContext) Less(left, right op.OP) bool {
	return c.est.Less(left, right)
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
