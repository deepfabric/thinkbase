package right

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
)

type right struct {
	e    extend.Extend
	a, b relation.Relation
}
