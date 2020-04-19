package build

import (
	"github.com/deepfabric/thinkbase/pkg/sql/tree"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
)

func (b *build) buildWhere(n *tree.Where) (extend.Extend, error) {
	if n == nil {
		return nil, nil
	}
	return b.buildExpr(n.E, Where)
}
