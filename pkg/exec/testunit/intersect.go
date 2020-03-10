package testunit

import (
	"errors"
	"sync"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func newIntersect(n int, c context.Context, a, b relation.Relation) ([]unit.Unit, error) {
	if len(a.Metadata()) != len(b.Metadata()) {
		return nil, errors.New("size is different")
	}
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
		ts, err := util.GetTuples(a)
		if err != nil {
			return nil, err
		}
		mp := new(sync.Map)
		for _, t := range ts {
			mp.Store(t.String(), nil)
		}
		var us []unit.Unit
		for i, j := 0, len(rs); i < j; i++ {
			us = append(us, &intersectUnit{mp, c, rs[i], a})
		}
		return us, nil
	default:
		rs, err := a.Split(n)
		if err != nil {
			return nil, err
		}
		ts, err := util.GetTuples(b)
		if err != nil {
			return nil, err
		}
		mp := new(sync.Map)
		for _, t := range ts {
			mp.Store(t.String(), nil)
		}
		var us []unit.Unit
		for i, j := 0, len(rs); i < j; i++ {
			us = append(us, &intersectUnit{mp, c, rs[i], b})
		}
		return us, nil
	}
}

func (u *intersectUnit) Result() (relation.Relation, error) {
	ts, err := util.GetTuples(u.a)
	if err != nil {
		return nil, err
	}
	r := mem.New("", u.a.Metadata(), u.c)
	for _, t := range ts {
		if _, ok := u.mp.Load(t.String()); ok {
			r.AddTuple(t)
		}
	}
	return r, nil
}
