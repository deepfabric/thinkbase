package testunit

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func New(n, op int, c context.Context, a, b relation.Relation) ([]unit.Unit, error) {
	switch op {
	case unit.Minus:
		return newMinus(n, c, a, b)
		/*
			case unit.Union:
				return newUnion(n, a, b)
			case unit.Product:
				return newProduct(n, a, b)
			case unit.Intersect:
				return newIntersect(n, a, b)
		*/
	}
	return nil, nil
}
