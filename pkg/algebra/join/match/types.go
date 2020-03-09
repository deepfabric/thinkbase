package match

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
)

// semi join
type match struct {
	c    context.Context
	a, b relation.Relation
}
