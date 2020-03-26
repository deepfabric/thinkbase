package context

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/counter"
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictVec"
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictionary"
	"github.com/deepfabric/thinkbase/pkg/vm/container/hash"
	"github.com/deepfabric/thinkbase/pkg/vm/container/vector"
	"github.com/deepfabric/thinkbase/pkg/vm/estimator"
)

type Context interface {
	estimator.Estimator
	NumMcpu() int  // 逻辑执行单元限制
	NumRcpu() int  // 执行单元的最佳并行读数目
	MemSize() int  // 可用内存限制
	DiskSize() int // 可用磁盘限制

	NewHash(int) (hash.Hash, error)
	NewVector() (vector.Vector, error)
	NewCounter() (counter.Counter, error)
	NewDictVector() (dictVec.DictVector, error)
	NewDictionary() (dictionary.Dictionary, error)
}
