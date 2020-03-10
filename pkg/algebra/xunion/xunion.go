package xunion

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/intersect"
	"github.com/deepfabric/thinkbase/pkg/algebra/minus"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/union"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(c context.Context, a, b relation.Relation) *xunion {
	return &xunion{c, a, b}
}

func (u *xunion) Xunion() (relation.Relation, error) {
	l, err := union.New(u.c, u.a, u.b).Union()
	if err != nil {
		return nil, err
	}
	r, err := intersect.New(u.c, u.a, u.b).Intersect()
	if err != nil {
		return nil, err
	}
	return minus.New(u.c, l, r).Minus()
}
