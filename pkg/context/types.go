package context

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
)

type Context interface {
	Placeholder() int
	Relation(int) relation.Relation
	AddRelation(relation.Relation)
}

type context struct {
	sync.Mutex
	placeholder int
	mp          sync.Map
}
