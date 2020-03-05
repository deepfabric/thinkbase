package xunion

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/intersect"
	"github.com/deepfabric/thinkbase/pkg/algebra/minus"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/union"
)

func New(a, b relation.Relation) *xunion {
	return &xunion{a, b}
}

func (u *xunion) Xunion() (relation.Relation, error) {
	l, err := union.New(true, u.a, u.b).Union()
	if err != nil {
		return nil, err
	}
	r, err := intersect.New(u.a, u.b).Intersect()
	if err != nil {
		return nil, err
	}
	return minus.New(l, r).Minus()
}
