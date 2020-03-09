package full

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/join/left"
	"github.com/deepfabric/thinkbase/pkg/algebra/join/right"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/union"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(e extend.Extend, c context.Context, a, b relation.Relation) *full {
	return &full{e, c, a, b}
}

func (j *full) Join() (relation.Relation, error) {
	l, err := left.New(j.e, j.c, j.a, j.b).Join()
	if err != nil {
		return nil, err
	}
	r, err := right.New(j.e, j.c, j.a, j.b).Join()
	if err != nil {
		return nil, err
	}
	return union.New(true, j.c, l, r).Union()
}
