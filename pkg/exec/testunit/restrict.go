package testunit

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/restrict"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func NewRestrict(n int, e extend.Extend, r relation.Relation) ([]unit.Unit, error) {
	if !e.IsLogical() {
		return nil, errors.New("extend must be a boolean expression")
	}
	rs, err := r.Split(n)
	if err != nil {
		return nil, err
	}
	var us []unit.Unit
	for i, j := 0, len(rs); i < j; i++ {
		us = append(us, &restrictUnit{e, rs[i]})
	}
	return us, nil
}

func (u *restrictUnit) Result() (relation.Relation, error) {
	return restrict.New(u.e, u.r).Restrict()
}
