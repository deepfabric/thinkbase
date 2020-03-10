package testunit

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/join/product"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func newProduct(n int, c context.Context, a, b relation.Relation) ([]unit.Unit, error) {
	an, err := a.GetTupleCount()
	if err != nil {
		return nil, err
	}
	bn, err := b.GetTupleCount()
	if err != nil {
		return nil, err
	}
	switch {
	case an*len(a.Metadata()) < bn*len(b.Metadata()):
		rs, err := b.Split(n)
		if err != nil {
			return nil, err
		}
		var us []unit.Unit
		for i, j := 0, len(rs); i < j; i++ {
			us = append(us, &productUnit{c, a, rs[i]})
		}
		return us, nil
	default:
		rs, err := a.Split(n)
		if err != nil {
			return nil, err
		}
		var us []unit.Unit
		for i, j := 0, len(rs); i < j; i++ {
			us = append(us, &productUnit{c, rs[i], b})
		}
		return us, nil
	}
}

func (u *productUnit) Result() (relation.Relation, error) {
	return product.New(u.c, u.a, u.b).Join()
}
