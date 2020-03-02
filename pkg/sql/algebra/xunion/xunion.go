package xunion

import (
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/intersect"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/minus"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/union"
)

func New(isNub bool, a, b relation.Relation) *xunion {
	return &xunion{isNub, a, b}
}

func (u *xunion) Cost() int64 {
	return 0
}

func (u *xunion) Xunion() (relation.Relation, error) {
	r0, err := union.New(u.isNub, u.a, u.b).Union()
	if err != nil {
		return nil, err
	}
	r1, err := intersect.New(u.isNub, u.a, u.b).Intersect()
	if err != nil {
		return nil, err
	}
	return minus.New(u.isNub, r0, r1).Minus()
}
