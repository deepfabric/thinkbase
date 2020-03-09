package util

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func Dup(r relation.Relation, ct context.Context) (relation.Relation, error) {
	ts, err := GetTuples(r)
	if err != nil {
		return nil, err
	}
	rr := mem.New(r.Name(), r.Metadata(), ct)
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

func Getattribute(plh int, attrs map[int][]string, ct context.Context) (map[int]map[string]int, []value.Attribute, error) {
	var as []value.Attribute

	mp := make(map[int]map[string]int)
	r := ct.Relation(plh)
	mp[plh] = make(map[string]int)
	for i, attr := range attrs[plh] {
		mp[plh][attr] = i
		a, err := r.GetAttribute(attr)
		if err != nil {
			return nil, nil, err
		}
		as = append(as, a)
	}
	return mp, as, nil
}

func GetattributeByJoin(aplh, bplh int, attrs map[int][]string, ct context.Context) (map[int]map[string]int, []value.Attribute, []value.Attribute, error) {
	var as, bs []value.Attribute

	mp := make(map[int]map[string]int)
	{
		r := ct.Relation(aplh)
		mp[aplh] = make(map[string]int)
		for i, attr := range attrs[aplh] {
			mp[aplh][attr] = i
			a, err := r.GetAttribute(attr)
			if err != nil {
				return nil, nil, nil, err
			}
			as = append(as, a)
		}
	}
	{
		r := ct.Relation(bplh)
		mp[bplh] = make(map[string]int)
		for i, attr := range attrs[bplh] {
			mp[bplh][attr] = i
			a, err := r.GetAttribute(attr)
			if err != nil {
				return nil, nil, nil, err
			}
			bs = append(bs, a)
		}
	}
	return mp, as, bs, nil
}
