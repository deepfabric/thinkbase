package left

import (
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

type left struct {
	e    extend.Extend
	a, b relation.Relation
}
