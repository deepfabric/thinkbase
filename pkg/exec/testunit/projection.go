package testunit

import (
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/projection"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
)

func NewProjection(n int, as []*projection.Attribute, r relation.Relation) ([]unit.Unit, error) {
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
		u := relation.New("", nil, r.Metadata())
		cnt := step
		if cnt > rn-i {
			cnt = rn - i
		}
		ts, err := r.GetTuples(i, i+cnt)
		if err != nil {
			return nil, err
		}
		u.AddTuples(ts)
		us = append(us, &projectionUnit{u, as})
	}
	return us, nil
}

func (u *projectionUnit) Result() (relation.Relation, error) {
	return projection.New(u.r, u.as).Projection()
}
