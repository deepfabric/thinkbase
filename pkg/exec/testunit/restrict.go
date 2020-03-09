package testunit

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/restrict"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func NewRestrict(n int, e extend.Extend, c context.Context, r relation.Relation) ([]unit.Unit, error) {
	if !e.IsLogical() {
		return nil, errors.New("extend must be a boolean expression")
	}
	rs, err := r.Split(n)
	if err != nil {
		return nil, err
	}
	var us []unit.Unit
	plh := r.Placeholder()
	for i, j := 0, len(rs); i < j; i++ {
		us = append(us, &restrictUnit{plh, e, c, rs[i]})
	}
	return us, nil
}

func (u *restrictUnit) Result() (relation.Relation, error) {
	mp := make(map[int]int)
	mp[u.plh] = u.r.Placeholder()
	return restrict.New(extend.Dup(u.e, mp), u.c, u.r).Restrict()
}
