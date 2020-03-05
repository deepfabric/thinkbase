package left

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
)

func New(e extend.Extend, a, b relation.Relation) *left {
	return &left{e, a, b}
}

func (j *left) Join() (relation.Relation, error) {
	if !j.e.IsLogical() {
		return nil, errors.New("extend must be a boolean expression")
	}
	as, err := util.GetTuples(j.a)
	if err != nil {
		return nil, err
	}
	bs, err := util.GetTuples(j.b)
	if err != nil {
		return nil, err
	}
	r := mem.New("", util.GetMetadata(j.a, j.b))
	pad := padding(j.b.Metadata())
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
	return r, nil
}

func padding(as []string) value.Tuple {
	var t value.Tuple

	for i, j := 0, len(as); i < j; i++ {
		t = append(t, value.ConstNull)
	}
	return t
}
