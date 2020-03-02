package inner

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/sql/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
)

func New(e extend.Extend, a, b relation.Relation) *inner {
	return &inner{e, a, b}
}

func (j *inner) Join() (relation.Relation, error) {
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
	r := relation.New("", nil, util.GetMetadata(j.a, j.b))
	for _, a := range as {
		for _, b := range bs {
			ok, err := j.e.Eval([]value.Tuple{a, b})
			if err != nil {
				return nil, err
			}
			if value.MustBeBool(ok) {
				r.AddTuple(append(a, b...))
			}
		}
	}
	return r, nil
}
