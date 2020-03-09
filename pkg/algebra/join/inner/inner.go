package inner

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(e extend.Extend, c context.Context, a, b relation.Relation) *inner {
	return &inner{e, c, a, b}
}

func (j *inner) Join() (relation.Relation, error) {
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
	for ia := 0; ia < acnt; ia++ {
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
	}
	return r, nil
}
