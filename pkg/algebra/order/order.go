package order

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(isNub bool, descs []bool, attrs []string, c context.Context, r relation.Relation) *order {
	return &order{isNub, descs, attrs, c, r}
}

func (o *order) Order() (relation.Relation, error) {
	r, err := util.Dup(o.r, o.c)
	if err != nil {
		return nil, err
	}
	if err := r.Sort(o.attrs, o.descs); err != nil {
		return nil, err
	}
	if o.isNub {
		if err := r.Nub(); err != nil {
			return nil, err
		}
	}
	return r, nil
}
