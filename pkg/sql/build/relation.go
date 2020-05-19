package build

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/sql/tree"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

func (b *build) buildRelation(n tree.RelationStatement) (op.OP, error) {
	b.ts = append([]*tables{&tables{}}, b.ts...)
	defer func() {
		b.ts = b.ts[1:]
	}()
	switch t := n.(type) {
	case *tree.AliasedTable:
		return nil, fmt.Errorf("'%s' not support now", n)
	case *tree.JoinClause:
		return nil, fmt.Errorf("'%s' not support now", n)
	case *tree.UnionClause:
		return nil, fmt.Errorf("'%s' not support now", n)
	case *tree.SelectClause:
		return b.buildSelect(t)
	case *tree.AliasedSelect:
		return nil, fmt.Errorf("'%s' not support now", n)
	default:
		return nil, fmt.Errorf("unknown relation statement '%s'", n)
	}
}
