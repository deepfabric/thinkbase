package difference

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/counter"
	"github.com/deepfabric/thinkbase/pkg/vm/context"

	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

type difference struct {
	isCheck   bool
	isLeftMin bool
	left      op.OP
	right     op.OP
	ctr       counter.Counter
	c         context.Context
}
