package union

import (
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

type union struct {
	isCheck  bool
	isREmpty bool
	isSEmpty bool
	r        op.OP
	s        op.OP
	rts      value.Array
	sts      value.Array
	c        context.Context
	lt       func(value.Value, value.Value) bool
}
