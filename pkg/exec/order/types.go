package order

import (
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
)

type Order interface {
	Order() (relation.Relation, error)
}

type order struct {
	us  []unit.Unit
	cmp func(value.Tuple, value.Tuple) bool
}
