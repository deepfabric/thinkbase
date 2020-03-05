package full

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
)

type full struct {
	e    extend.Extend
	a, b relation.Relation
}
