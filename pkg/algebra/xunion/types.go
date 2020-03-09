package xunion

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type Xunion interface {
	Xunion() relation.Relation
}

type xunion struct {
	c    context.Context
	a, b relation.Relation
}
