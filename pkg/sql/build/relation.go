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
	case *tree.TableName:
		tbl, err := b.buildTableName(t)
		if err != nil {
			return nil, err
		}
		return tbl.r, nil
	case *tree.JoinClause:
		return b.buildJoin(t)
	case *tree.UnionClause:
		return b.buildUnion(t)
	case *tree.SelectClause:
		return b.buildSelect(t)
	case *tree.AliasedSelect:
		return b.buildAliasedSelect(t)
	default:
		return nil, fmt.Errorf("unknown relation statement '%s'", n)
	}
}
