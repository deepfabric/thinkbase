package intersect

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

type Intersect interface {
	Intersect() (relation.Relation, error)
}

type intersect struct {
	us []unit.Unit
}
