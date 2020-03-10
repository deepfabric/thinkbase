package order

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type Order interface {
	Order() (relation.Relation, error)
}

type order struct {
	c  context.Context
	r  relation.Relation
	lt func(value.Tuple, value.Tuple) bool
}

type tuples struct {
	tuple []value.Tuple
	lt    func(value.Tuple, value.Tuple) bool
}

func (t tuples) Len() int           { return len(t.tuple) }
func (t tuples) Less(i, j int) bool { return t.lt(t.tuple[i], t.tuple[j]) }
func (t tuples) Swap(i, j int)      { t.tuple[i], t.tuple[j] = t.tuple[j], t.tuple[i] }
