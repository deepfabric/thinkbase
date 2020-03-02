package order

import (
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/util"
)

func New(isNub bool, descs []bool, attrs []string, r relation.Relation) *order {
	return &order{isNub, descs, attrs, r}
}

func (o *order) Order() (relation.Relation, error) {
	r, err := util.Dup(o.r)
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
