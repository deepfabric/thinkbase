package product

import (
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/util"
)

func New(a, b relation.Relation) *product {
	return &product{a, b}
}

func (j *product) Join() (relation.Relation, error) {
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
			r.AddTuple(append(a, b...))
		}
	}
	return r, nil
}