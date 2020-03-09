package right

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(e extend.Extend, c context.Context, a, b relation.Relation) *right {
	return &right{e, c, a, b}
}

func (j *right) Join() (relation.Relation, error) {
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
	pad := padding(j.a.Metadata())
	for ib := 0; ib < acnt; ib++ {
		added := false
		for ia := 0; ia < bcnt; ia++ {
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
			b, err := j.b.GetTuple(ib)
			if err != nil {
				return nil, err
			}
			r.AddTuple(append(pad, b...))
		}
	}
	return r, nil
}

func padding(as []string) value.Tuple {
	var t value.Tuple

	for i, j := 0, len(as); i < j; i++ {
		t = append(t, value.ConstNull)
	}
	return t
}
