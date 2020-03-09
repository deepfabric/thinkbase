package product

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type product struct {
	c    context.Context
	a, b relation.Relation
}
