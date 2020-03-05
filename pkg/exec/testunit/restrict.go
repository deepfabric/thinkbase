package testunit

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/restrict"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func NewRestrict(n int, e extend.Extend, r relation.Relation) ([]unit.Unit, error) {
	if !e.IsLogical() {
		return nil, errors.New("extend must be a boolean expression")
	}
	rn, err := r.GetTupleCount()
	if err != nil {
		return nil, err
	}
	var us []unit.Unit
	step := rn / n
	if step < 1 {
		step = 1
	}
	for i := 0; i < rn; i += step {
		u := mem.New("", r.Metadata())
		cnt := step
		if cnt > rn-i {
			cnt = rn - i
		}
		ts, err := r.GetTuples(i, i+cnt)
		if err != nil {
			return nil, err
		}
		u.AddTuples(ts)
		us = append(us, &restrictUnit{e, u})
	}
	return us, nil
}

func (u *restrictUnit) Result() (relation.Relation, error) {
	return restrict.New(u.e, u.r).Restrict()
}
