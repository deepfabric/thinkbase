package testunit

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/algebra/nub"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func NewNub(n int, c context.Context, r relation.Relation) ([]unit.Unit, error) {
	rs, err := r.Split(n)
	if err != nil {
		return nil, err
	}
	var us []unit.Unit
	mp := new(sync.Map)
	for i, j := 0, len(rs); i < j; i++ {
		us = append(us, &nubUnit{mp, c, rs[i]})
	}
	return us, nil
}

func (u *nubUnit) Result() (relation.Relation, error) {
	return nub.New(u.mp, u.c, u.r).Nub()
}
