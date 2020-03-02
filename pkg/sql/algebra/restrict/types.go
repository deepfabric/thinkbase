package restrict

import (
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

type Restrict interface {
	Restrict() (relation.Relation, error)
}

type restrict struct {
	e extend.Extend
	r relation.Relation
}
