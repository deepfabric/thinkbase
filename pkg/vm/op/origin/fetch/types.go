package fetch

import (
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

type fetch struct {
	isCheck       bool
	cnt, off      int
	limit, offset int
	prev          op.OP
	c             context.Context
}
