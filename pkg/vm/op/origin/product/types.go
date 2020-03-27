package product

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/vector"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

type product struct {
	isCheck bool
	left    op.OP
	right   op.OP
	v       vector.Vector
	c       context.Context
}
