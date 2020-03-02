package inner

import (
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

type inner struct {
	e    extend.Extend
	a, b relation.Relation
}
