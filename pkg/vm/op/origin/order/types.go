package order

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/dictVector"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

type OrderOP interface {
	op.OP
	NewLt([]string) func(value.Value, value.Value) bool
	NewCmp([]string) func(value.Value, value.Value) int
}

type order struct {
	isCheck bool
	prev    op.OP
	descs   []bool
	id      string
	attrs   []string
	c       context.Context
	dv      dictVector.DictVector
}

type tuples struct {
	a  value.Array
	lt func(value.Value, value.Value) bool
}

func (t tuples) Len() int           { return len(t.a) }
func (t tuples) Less(i, j int) bool { return t.lt(t.a[i], t.a[j]) }
func (t tuples) Swap(i, j int)      { t.a[i], t.a[j] = t.a[j], t.a[i] }
