package build

import (
	"errors"
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/sql/tree"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/join/inner"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/join/natural"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/product"
)

func (b *build) buildJoin(n *tree.JoinClause) (op.OP, error) {
	r, err := b.buildRelation(n.Left)
	if err != nil {
		return nil, err
	}
	s, err := b.buildRelation(n.Right)
	if err != nil {
		return nil, err
	}
	switch {
	case n.Type == tree.FullOp:
		return nil, errors.New("full join not support now")
	case n.Type == tree.LeftOp:
		return nil, errors.New("left join not support now")
	case n.Type == tree.RightOp:
		return nil, errors.New("right join not support now")
	case n.Type == tree.CrossOp:
		return product.New(r, s, b.c), nil
	case n.Type == tree.InnerOp:
		e, err := b.buildConditionWithoutSubquery(n.Cond.(*tree.OnJoinCond).E)
		if err != nil {
			return nil, err
		}
		return inner.New(r, s, e, b.c), nil
	case n.Type == tree.NaturalOp:
		return natural.New(r, s, b.c), nil
	}
	return nil, fmt.Errorf("unsupport join '%s'", n)
}
