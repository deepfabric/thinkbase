package testunit

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/minus"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func newMinus(n int, c context.Context, a, b relation.Relation) ([]unit.Unit, error) {
	if len(a.Metadata()) != len(b.Metadata()) {
		return nil, errors.New("size is different")
	}
	rs, err := b.Split(n)
	if err != nil {
		return nil, err
	}
	var us []unit.Unit
	for i, j := 0, len(rs); i < j; i++ {
		us = append(us, &minusUnit{c, a, rs[i]})
	}
	return us, nil
}

func (u *minusUnit) Result() (relation.Relation, error) {
	return minus.New(u.c, u.a, u.b).Minus()
}
