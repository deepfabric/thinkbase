package restrict

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

type Restrict interface {
	Restrict() (relation.Relation, error)
}

type restrict struct {
	us []unit.Unit
	c  context.Context
}
