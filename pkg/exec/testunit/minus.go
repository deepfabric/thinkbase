package testunit

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/minus"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func newMinus(n int, a, b relation.Relation) ([]unit.Unit, error) {
	if len(a.Metadata()) != len(b.Metadata()) {
		return nil, errors.New("size is different")
	}
	bn, err := a.GetTupleCount()
	if err != nil {
		return nil, err
	}
	var us []unit.Unit
	step := bn / n
	if step < 1 {
		step = 1
	}
	for i := 0; i < bn; i += step {
		r := mem.New("", b.Metadata())
		cnt := step
		if cnt > bn-i {
			cnt = bn - i
		}
		ts, err := b.GetTuples(i, i+cnt)
		if err != nil {
			return nil, err
		}
		r.AddTuples(ts)
		us = append(us, &minusUnit{a, r})
	}
	return us, nil
}

func (u *minusUnit) Result() (relation.Relation, error) {
	return minus.New(u.a, u.b).Minus()
}
