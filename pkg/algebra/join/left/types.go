package left

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
)

type left struct {
	e    extend.Extend
	a, b relation.Relation
}
