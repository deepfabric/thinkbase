package restrict

import (
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

type restrict struct {
	isCheck bool
	prev    op.OP
	e       extend.Extend
	c       context.Context
}
