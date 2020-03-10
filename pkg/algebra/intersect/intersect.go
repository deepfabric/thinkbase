package intersect

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(c context.Context, a, b relation.Relation) *intersect {
	return &intersect{c, a, b}
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
	r := mem.New("", i.a.Metadata(), i.c)
	mp := make(map[string]struct{})
	for _, b := range bs {
		mp[b.String()] = struct{}{}
	}
	for _, a := range as {
		if _, ok := mp[a.String()]; ok {
			r.AddTuple(a)
		}
	}
	return r, nil
}
