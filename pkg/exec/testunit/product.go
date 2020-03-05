package testunit

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/join/product"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func newProduct(n int, a, b relation.Relation) ([]unit.Unit, error) {
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
			us = append(us, &productUnit{a, r})
		}
		return us, nil
	}
	step := an / n
	if step < 1 {
		step = 1
	}
	for i := 0; i < an; i += step {
		r := mem.New("", a.Metadata())
		cnt := step
		if cnt > an-i {
			cnt = an - i
		}
		ts, err := a.GetTuples(i, i+cnt)
		if err != nil {
			return nil, err
		}
		r.AddTuples(ts)
		us = append(us, &productUnit{r, b})
	}
	return us, nil

}

func (u *productUnit) Result() (relation.Relation, error) {
	return product.New(u.a, u.b).Join()
}
