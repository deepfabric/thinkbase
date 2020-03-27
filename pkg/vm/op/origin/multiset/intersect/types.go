package intersect

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/counter"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

type intersect struct {
	isCheck bool
	left    op.OP
	right   op.OP
	ctr     counter.Counter
	c       context.Context
}
