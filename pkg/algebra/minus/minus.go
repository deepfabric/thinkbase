package minus

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
)

func New(a, b relation.Relation) *minus {
	return &minus{a, b}
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
	r := mem.New("", m.a.Metadata())
	for _, a := range as {
		ok := true
		for _, b := range bs {
			if a.Compare(b) == 0 {
				ok = false
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
