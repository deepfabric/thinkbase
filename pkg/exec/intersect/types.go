package intersect

import (
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

type Intersect interface {
	Intersect() (relation.Relation, error)
}

type intersect struct {
	us []unit.Unit
}
