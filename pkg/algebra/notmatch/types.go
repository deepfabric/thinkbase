package notmatch

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type notmatch struct {
	c    context.Context
	a, b relation.Relation
}
