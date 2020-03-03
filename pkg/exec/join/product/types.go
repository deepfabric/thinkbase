package product

import (
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

type Product interface {
	Product() (relation.Relation, error)
}

type product struct {
	us []unit.Unit
}
