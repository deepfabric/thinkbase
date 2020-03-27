package t2a

import (
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

type t2a struct {
	isCheck bool
	prev    op.OP
	c       context.Context
}
