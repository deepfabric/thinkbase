package projection

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictVec"
	"github.com/deepfabric/thinkbase/pkg/vm/container/vector"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

type projection struct {
	isCheck bool
	ops     []op.OP
	c       context.Context
	vs      []vector.Vector
	dvs     []dictVec.DictVector
}
