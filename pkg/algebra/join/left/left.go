package left

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(e extend.Extend, c context.Context, a, b relation.Relation) *left {
	return &left{e, c, a, b}
}

func (j *left) Join() (relation.Relation, error) {
	if !j.e.IsLogical() {
		return nil, errors.New("extend must be a boolean expression")
	}
	acnt, err := j.a.GetTupleCount()
	if err != nil {
		return nil, err
	}
	bcnt, err := j.b.GetTupleCount()
	if err != nil {
		return nil, err
	}
	mp, as, bs, err := util.GetattributeByJoin(j.a.Placeholder(), j.b.Placeholder(), j.e.Attributes(), j.c)
	if err != nil {
		return nil, err
	}
	r := mem.New("", util.GetMetadata(j.a, j.b), j.c)
	pad := padding(j.b.Metadata())
	for ia := 0; ia < acnt; ia++ {
		added := false
		for ib := 0; ib < bcnt; ib++ {
			var at, bt value.Tuple
			for _, attrs := range as {
				at = append(at, attrs[ia])
			}
			for _, attrs := range bs {
				bt = append(bt, attrs[ib])
			}
			ok, err := j.e.Eval([]value.Tuple{at, bt}, mp)
			if err != nil {
				return nil, err
			}
			if value.MustBeBool(ok) {
				added = true
				a, err := j.a.GetTuple(ia)
				if err != nil {
					return nil, err
				}
				b, err := j.b.GetTuple(ib)
				if err != nil {
					return nil, err
				}
				r.AddTuple(append(a, b...))
			}
		}
		if !added {
			a, err := j.a.GetTuple(ia)
			if err != nil {
				return nil, err
			}
			r.AddTuple(append(a, pad...))
		}
	}
	/*
		for _, a := range as {
			added := false
			for _, b := range bs {
				ok, err := j.e.Eval([]value.Tuple{a, b})
				if err != nil {
					return nil, err
				}
				if value.MustBeBool(ok) {
					added = true
					r.AddTuple(append(a, b...))
				}
			}
			if !added {
				r.AddTuple(append(a, pad...))
			}
		}
	*/
	return r, nil
}

func padding(as []string) value.Tuple {
	var t value.Tuple

	for i, j := 0, len(as); i < j; i++ {
		t = append(t, value.ConstNull)
	}
	return t
}
