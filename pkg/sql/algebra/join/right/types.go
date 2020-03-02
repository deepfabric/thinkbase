package right

import (
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

type right struct {
	e    extend.Extend
	a, b relation.Relation
}
