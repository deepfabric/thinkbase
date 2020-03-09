package testunit

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/order"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func NewOrder(n int, isNub bool, descs []bool, attrs []string, c context.Context, r relation.Relation) ([]unit.Unit, error) {
	rs, err := r.Split(n)
	if err != nil {
		return nil, err
	}
	var us []unit.Unit
	for i, j := 0, len(rs); i < j; i++ {
		us = append(us, &orderUnit{isNub, descs, attrs, c, rs[i]})
	}
	return us, nil
}

func (u *orderUnit) Result() (relation.Relation, error) {
	return order.New(u.isNub, u.descs, u.attrs, u.c, u.r).Order()
}
