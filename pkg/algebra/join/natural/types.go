package natural

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type natural struct {
	c    context.Context
	a, b relation.Relation
}
