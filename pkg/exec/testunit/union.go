package testunit

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/exec/unit"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

func newUnion(n int, a, b relation.Relation) ([]unit.Unit, error) {
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
	step := (an + bn) / n
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
		us = append(us, &unionUnit{r})
	}
	for i := 0; i < bn; i += step {
		r := relation.New("", nil, a.Metadata())
		cnt := step
		if cnt > bn-i {
			cnt = bn - i
		}
		ts, err := b.GetTuples(i, i+cnt)
		if err != nil {
			return nil, err
		}
		r.AddTuples(ts)
		us = append(us, &unionUnit{r})
	}
	return us, nil
}

func (u *unionUnit) Result() (relation.Relation, error) {
	return u.a, nil
}