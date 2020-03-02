package minus

import (
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

type Minus interface {
	Minus() (relation.Relation, error)
}

type minus struct {
	us []unit.Unit
}
