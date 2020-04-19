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
		{
			o := product.New(r, s, b.c)
			attrs, err := o.AttributeList()
			if err != nil {
				return nil, err
			}
			b.ts[0].ts = append(b.ts[0].ts, &table{o: o, attrs: attrs})
		}
		e, err := b.buildOn(n.Cond.(*tree.OnJoinCond))
		if err != nil {
			return nil, err
		}
		if !e.IsLogical() {
			return nil, fmt.Errorf("'%s' is not logical expression", e)
		}
		return inner.New(r, s, e, b.c), nil
	case n.Type == tree.NaturalOp:
		return natural.New(r, s, b.c), nil
	}
	return nil, fmt.Errorf("unsupport join '%s'", n)
}
