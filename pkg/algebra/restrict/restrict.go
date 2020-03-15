package restrict

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(e extend.Extend, c context.Context, r relation.Relation) *restrict {
	return &restrict{e, c, r}
}

func (r *restrict) Restrict() (relation.Relation, error) {
	if !r.e.IsLogical() {
		return nil, errors.New("extend must be a boolean expression")
	}
	cnt, err := r.r.GetTupleCount()
	if err != nil {
		return nil, err
	}
	mp, as, err := util.Getattribute(r.r.Placeholder(), r.e.Attributes(), r.c)
	if err != nil {
		return nil, err
	}
	is := []int{}
	rr := mem.New("", r.r.Metadata(), r.c)
	for i := 0; i < cnt; i++ {
		var et value.Tuple
		for _, attrs := range as {
			et = append(et, attrs[i])
		}
		ok, err := r.e.Eval([]value.Tuple{et, et}, mp)
		if err != nil {
			return nil, err
		}
		if value.MustBeBool(ok) {
			is = append(is, i)
		}
	}
	if len(is) > 0 {
		ts, err := r.r.GetTuplesByIndex(is)
		if err != nil {
			return nil, err
		}
		rr.AddTuples(ts)
	}
	return rr, nil
}
