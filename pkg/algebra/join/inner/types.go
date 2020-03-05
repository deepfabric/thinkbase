package inner

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
)

type inner struct {
	e    extend.Extend
	a, b relation.Relation
}
