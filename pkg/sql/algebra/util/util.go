package util

import (
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
)

func Dup(r relation.Relation) (relation.Relation, error) {
	ts, err := GetTuples(r)
	if err != nil {
		return nil, err
	}
	rr := relation.New(r.Name(), nil, DupMetadata(r.Metadata()))
	rr.AddTuples(ts)
	return rr, nil
}

func DupMetadata(as []*relation.AttributeMetadata) []*relation.AttributeMetadata {
	var attrs []*relation.AttributeMetadata

	for _, a := range as {
		attrs = append(attrs, &relation.AttributeMetadata{a.Name, make(map[int32]int)})
	}
	return attrs
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

func GetMetadata(a, b relation.Relation) []*relation.AttributeMetadata {
	var attrs []*relation.AttributeMetadata

	as, bs := a.Metadata(), b.Metadata()
	for i, j := 0, len(as); i < j; i++ {
		as[i].Name = a.Name() + "." + as[i].Name
		attrs = append(attrs, &relation.AttributeMetadata{as[i].Name, make(map[int32]int)})
	}
	for i, j := 0, len(bs); i < j; i++ {
		bs[i].Name = b.Name() + "." + bs[i].Name
		attrs = append(attrs, &relation.AttributeMetadata{bs[i].Name, make(map[int32]int)})
	}
	return attrs
}
