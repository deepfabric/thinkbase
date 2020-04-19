package build

import (
	"github.com/deepfabric/thinkbase/pkg/sql/tree"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
)

func (b *build) buildHaving(n *tree.Where, g *tree.GroupBy) (extend.Extend, error) {
	if g == nil || n == nil {
		return nil, nil
	}
	return b.buildExpr(n.E, Having)
}
