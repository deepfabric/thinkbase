package a2t

import (
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

type a2t struct {
	isCheck bool
	prev    op.OP
	attrs   []string
	c       context.Context
}
