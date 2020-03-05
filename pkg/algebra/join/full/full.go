package full

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/join/left"
	"github.com/deepfabric/thinkbase/pkg/algebra/join/right"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/union"
)

func New(e extend.Extend, a, b relation.Relation) *full {
	return &full{e, a, b}
}

func (j *full) Join() (relation.Relation, error) {
	l, err := left.New(j.e, j.a, j.b).Join()
	if err != nil {
		return nil, err
	}
	r, err := right.New(j.e, j.a, j.b).Join()
	if err != nil {
		return nil, err
	}
	return union.New(true, l, r).Union()
}
