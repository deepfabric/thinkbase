package build

import (
	"github.com/deepfabric/thinkbase/pkg/sql/tree"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
)

func (b *build) buildOn(n *tree.OnJoinCond) (extend.Extend, error) {
	return b.buildExpr(n.E, On)
}
