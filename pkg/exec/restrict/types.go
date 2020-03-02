package restrict

import (
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

type Restrict interface {
	Restrict() (relation.Relation, error)
}

type restrict struct {
	us []unit.Unit
}
