package product

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(c context.Context, a, b relation.Relation) *product {
	return &product{c, a, b}
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
	r := mem.New("", util.GetMetadata(j.a, j.b), j.c)
	for _, a := range as {
		for _, b := range bs {
			r.AddTuple(append(a, b...))
		}
	}
	return r, nil
}
