package rename

import (
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

type rename struct {
	isCheck bool
	prev    op.OP
	name    string
	c       context.Context
	mp      map[string]string
	mq      map[string]string
}
