package build

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/sql/tree"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	mdifference "github.com/deepfabric/thinkbase/pkg/vm/op/origin/multiset/difference"
	mintersect "github.com/deepfabric/thinkbase/pkg/vm/op/origin/multiset/intersect"
	munion "github.com/deepfabric/thinkbase/pkg/vm/op/origin/multiset/union"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/set/difference"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/set/intersect"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/set/union"
)

func (b *build) buildUnion(n *tree.UnionClause) (op.OP, error) {
	r, err := b.buildRelation(n.Left)
	if err != nil {
		return nil, err
	}
	s, err := b.buildRelation(n.Right)
	if err != nil {
		return nil, err
	}
	switch {
	case n.Type == tree.UnionOp:
		return union.New(r, s, b.c), nil
	case n.Type == tree.ExceptOp:
		return difference.New(r, s, b.c), nil
	case n.Type == tree.IntersectOp:
		return intersect.New(r, s, b.c), nil
	case n.Type == tree.UnionOp && n.All:
		return munion.New(r, s, b.c), nil
	case n.Type == tree.ExceptOp && n.All:
		return mdifference.New(r, s, b.c), nil
	case n.Type == tree.IntersectOp && n.All:
		return mintersect.New(r, s, b.c), nil
	}
	return nil, fmt.Errorf("unsupport union '%s'", n)
}
