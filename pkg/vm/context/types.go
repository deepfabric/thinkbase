package context

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictVector"
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictionary"
	"github.com/deepfabric/thinkbase/pkg/vm/container/mdictionary"
	"github.com/deepfabric/thinkbase/pkg/vm/container/mvector"

	"github.com/deepfabric/thinkbase/pkg/vm/estimator"
	"github.com/deepfabric/thinkbase/pkg/vm/workspace"
)

type Config struct {
	Mcpu      int
	Rcpu      int
	MemSize   int
	DiskSize  int
	BlockSize int
	Uid       string
}

type Context interface {
	estimator.Estimator
	workspace.Workspace

	NumMcpu() int   // 逻辑执行单元限制
	NumRcpu() int   // 执行单元的最佳并行读数目
	MemSize() int   // 可用内存限制
	DiskSize() int  // 可用磁盘限制
	BlockSize() int // 批量获取数据的块大小限制

	NewDictionary() (dictionary.Dictionary, error)
	NewDictVector() (dictVector.DictVector, error)

	NewMvector() (mvector.Mvector, error)
	NewMdictionary() (mdictionary.Mdictionary, error)
}

type context struct {
	mcpu      int
	rcpu      int
	memSize   int
	diskSize  int
	blockSize int
	uid       string
	est       estimator.Estimator
	wsp       workspace.Workspace
}
