package union

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/util"
)

func New(isNub bool, a, b relation.Relation) *union {
	return &union{isNub, a, b}
}

func (u *union) Union() (relation.Relation, error) {
	if len(u.A.Metadata()) != len(u.B.Metadata()) {
		return nil, errors.New("size is different")
	}
	as, err := util.GetTuples(u.A)
	if err != nil {
		return nil, err
	}
	bs, err := util.GetTuples(u.B)
	if err != nil {
		return nil, err
	}
	r := relation.New("", nil, util.DupMetadata(u.A.Metadata()))
	r.AddTuples(as)
	r.AddTuples(bs)
	if u.IsNub {
		if err := r.Nub(); err != nil {
			return nil, err
		}
	}
	return r, nil
}
