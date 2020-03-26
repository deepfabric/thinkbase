package projection

import (
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

type Extend struct {
	Alias string
	E     extend.Extend
}

type projection struct {
	isCheck bool
	prev    op.OP
	es      []*Extend
	c       context.Context
}
