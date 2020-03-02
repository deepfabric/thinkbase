package projection

import (
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

type Projection interface {
	Projection() (relation.Relation, error)
}

type projection struct {
	us []unit.Unit
}
