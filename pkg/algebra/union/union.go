package union

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(isNub bool, c context.Context, a, b relation.Relation) *union {
	return &union{isNub, c, a, b}
}

func (u *union) Union() (relation.Relation, error) {
	if len(u.a.Metadata()) != len(u.b.Metadata()) {
		return nil, errors.New("size is different")
	}
	as, err := util.GetTuples(u.a)
	if err != nil {
		return nil, err
	}
	bs, err := util.GetTuples(u.b)
	if err != nil {
		return nil, err
	}
	r := mem.New("", u.a.Metadata(), u.c)
	r.AddTuples(as)
	r.AddTuples(bs)
	if u.IsNub {
		if err := r.Nub(); err != nil {
			return nil, err
		}
	}
	return r, nil
}
