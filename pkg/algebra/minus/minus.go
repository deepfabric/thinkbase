package minus

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(c context.Context, a, b relation.Relation) *minus {
	return &minus{c, a, b}
}

func (m *minus) Minus() (relation.Relation, error) {
	if len(m.a.Metadata()) != len(m.b.Metadata()) {
		return nil, errors.New("size is different")
	}
	as, err := util.GetTuples(m.a)
	if err != nil {
		return nil, err
	}
	bs, err := util.GetTuples(m.b)
	if err != nil {
		return nil, err
	}
	r := mem.New("", m.a.Metadata(), m.c)
	mp := make(map[string]struct{})
	for _, b := range bs {
		mp[b.String()] = struct{}{}
	}
	for _, a := range as {
		if _, ok := mp[a.String()]; !ok {
			r.AddTuple(a)
		}
	}
	return r, nil
}
