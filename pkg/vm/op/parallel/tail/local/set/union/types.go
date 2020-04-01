package union

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/vector"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

type union struct {
	isCheck bool
	r       op.OP
	s       op.OP
	c       context.Context
	vs      []vector.Vector
}
