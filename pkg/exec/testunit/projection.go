package testunit

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/projection"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func NewProjection(n int, as []*projection.Attribute, c context.Context, r relation.Relation) ([]unit.Unit, error) {
	rs, err := r.Split(n)
	if err != nil {
		return nil, err
	}
	var us []unit.Unit
	plh := r.Placeholder()
	for i, j := 0, len(rs); i < j; i++ {
		us = append(us, &projectionUnit{plh, c, rs[i], as})
	}
	return us, nil
}

func (u *projectionUnit) Result() (relation.Relation, error) {
	var as []*projection.Attribute

	mp := make(map[int]int)
	mp[u.plh] = u.r.Placeholder()
	for _, a := range u.as {
		as = append(as, &projection.Attribute{
			Alias: a.Alias,
			E:     extend.Dup(a.E, mp),
		})
	}
	return projection.New(u.r, u.c, as).Projection()
}
