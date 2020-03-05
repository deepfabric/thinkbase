package minus

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

type Minus interface {
	Minus() (relation.Relation, error)
}

type minus struct {
	us []unit.Unit
}
