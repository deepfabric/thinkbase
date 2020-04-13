package build

import (
	"github.com/deepfabric/thinkbase/pkg/sql/tree"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/rename"
)

func (b *build) buildSelect(n *tree.SelectClause) (op.OP, error) {
	return nil, nil
}

func (b *build) buildAliasedSelect(n *tree.AliasedSelect) (op.OP, error) {
	o, err := b.buildStatement(n.Sel)
	if err != nil {
		return nil, err
	}
	return rename.New(o, string(n.As.Alias), make(map[string]string), b.c), nil
}
