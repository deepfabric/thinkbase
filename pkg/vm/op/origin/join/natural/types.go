package natural

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictVec"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

type join struct {
	isCheck bool
	left    op.OP
	right   op.OP
	lis     []int    //单独属性在left中的位置
	ris     []int    // 公共属性在right中的位置
	attrs   []string // 公共属性
	c       context.Context
	dv      dictVec.DictVector
}
