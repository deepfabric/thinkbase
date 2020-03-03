package restrict

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/sql/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
)

func New(e extend.Extend, r relation.Relation) *restrict {
	return &restrict{e, r}
}

func (r *restrict) Restrict() (relation.Relation, error) {
	if !r.e.IsLogical() {
		return nil, errors.New("extend must be a boolean expression")
	}
	ts, err := util.GetTuples(r.r)
	if err != nil {
		return nil, err
	}
	rr := relation.New("", nil, util.DupMetadata(r.r.Metadata()))
	for _, t := range ts {
		ok, err := r.e.Eval([]value.Tuple{t, t})
		if err != nil {
			return nil, err
		}
		if value.MustBeBool(ok) {
			rr.AddTuple(t)
		}
	}
	return rr, nil
}
