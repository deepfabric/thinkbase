package testunit

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/exec/unit"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/intersect"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

func newIntersect(n int, a, b relation.Relation) ([]unit.Unit, error) {
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
	var us []unit.Unit
	if an < bn {
		step := bn / n
		if step < 1 {
			step = 1
		}
		for i := 0; i < bn; i += step {
			r := relation.New("", nil, b.Metadata())
			cnt := step
			if cnt > bn-i {
				cnt = bn - i
			}
			ts, err := b.GetTuples(i, i+cnt)
			if err != nil {
				return nil, err
			}
			r.AddTuples(ts)
			us = append(us, &intersectUnit{a, r})
		}
		return us, nil
	}
	step := an / n
	if step < 1 {
		step = 1
	}
	for i := 0; i < an; i += step {
		r := relation.New("", nil, a.Metadata())
		cnt := step
		if cnt > an-i {
			cnt = an - i
		}
		ts, err := a.GetTuples(i, i+cnt)
		if err != nil {
			return nil, err
		}
		r.AddTuples(ts)
		us = append(us, &intersectUnit{b, r})
	}
	return us, nil
}

func (u *intersectUnit) Result() (relation.Relation, error) {
	return intersect.New(u.a, u.b).Intersect()
}
