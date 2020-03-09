package right

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type right struct {
	e    extend.Extend
	c    context.Context
	a, b relation.Relation
}
