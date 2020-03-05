package restrict

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
)

type Restrict interface {
	Restrict() (relation.Relation, error)
}

type restrict struct {
	e extend.Extend
	r relation.Relation
}
