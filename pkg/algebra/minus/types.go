package minus

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type Minus interface {
	Minus() relation.Relation
}

type minus struct {
	c    context.Context
	a, b relation.Relation
}
