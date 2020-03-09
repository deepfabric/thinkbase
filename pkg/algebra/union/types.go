package union

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type Union interface {
	relation.Relation
	Union(int) relation.Relation
}

type union struct {
	IsNub bool
	c     context.Context
	a, b  relation.Relation
}
