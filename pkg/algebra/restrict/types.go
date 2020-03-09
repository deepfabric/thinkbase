package restrict

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type Restrict interface {
	Restrict() (relation.Relation, error)
}

type restrict struct {
	e extend.Extend
	c context.Context
	r relation.Relation
}
