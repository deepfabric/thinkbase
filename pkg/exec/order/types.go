package order

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

type Order interface {
	Order() (relation.Relation, error)
}

type order struct {
	us  []unit.Unit
	c   context.Context
	cmp func(value.Tuple, value.Tuple) bool
}
