package nub

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
)

type Nub interface {
	Nub() (relation.Relation, error)
}

type nub struct {
	mp *sync.Map
	c  context.Context
	r  relation.Relation
}
