package testunit

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/order"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func NewOrder(n int, c context.Context, r relation.Relation, lt func(value.Tuple, value.Tuple) bool) ([]unit.Unit, error) {
	rs, err := r.Split(n)
	if err != nil {
		return nil, err
	}
	var us []unit.Unit
	for i, j := 0, len(rs); i < j; i++ {
		us = append(us, &orderUnit{c, rs[i], lt})
	}
	return us, nil
}

func (u *orderUnit) Result() (relation.Relation, error) {
	return order.New(u.c, u.r, u.lt).Order()
}
