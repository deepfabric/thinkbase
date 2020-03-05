package util

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
)

func Dup(r relation.Relation) (relation.Relation, error) {
	ts, err := GetTuples(r)
	if err != nil {
		return nil, err
	}
	rr := mem.New(r.Name(), r.Metadata())
	rr.AddTuples(ts)
	return rr, nil
}

func GetTuples(r relation.Relation) ([]value.Tuple, error) {
	num, err := r.GetTupleCount()
	if err != nil {
		return nil, err
	}
	ts, err := r.GetTuples(0, num)
	if err != nil {
		return nil, err
	}
	return ts, nil
}

func GetMetadata(a, b relation.Relation) []string {
	as, bs := a.Metadata(), b.Metadata()
	for i, j := 0, len(as); i < j; i++ {
		as[i] = a.Name() + "." + as[i]
	}
	for i, j := 0, len(bs); i < j; i++ {
		bs[i] = b.Name() + "." + bs[i]
	}
	return append(as, bs...)
}
