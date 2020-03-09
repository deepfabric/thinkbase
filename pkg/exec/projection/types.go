package projection

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

type Projection interface {
	Projection() (relation.Relation, error)
}

type projection struct {
	us []unit.Unit
	c  context.Context
}
