package intersect

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/util"
)

func New(a, b relation.Relation) *intersect {
	return &intersect{a, b}
}

func (i *intersect) Intersect() (relation.Relation, error) {
	if len(i.a.Metadata()) != len(i.b.Metadata()) {
		return nil, errors.New("size is different")
	}
	as, err := util.GetTuples(i.a)
	if err != nil {
		return nil, err
	}
	bs, err := util.GetTuples(i.b)
	if err != nil {
		return nil, err
	}
	r := relation.New("", nil, util.DupMetadata(i.a.Metadata()))
	for _, a := range as {
		ok := false
		for _, b := range bs {
			if a.Compare(b) == 0 {
				ok = true
				break
			}
		}
		if ok {
			r.AddTuple(a)
		}
	}
	if err := r.Nub(); err != nil {
		return nil, err
	}
	return r, nil
}
