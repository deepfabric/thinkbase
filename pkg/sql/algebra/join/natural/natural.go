package natural

import (
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
)

func New(a, b relation.Relation) *natural {
	return &natural{a, b}
}

func (j *natural) Join() (relation.Relation, error) {
	as, err := util.GetTuples(j.a)
	if err != nil {
		return nil, err
	}
	bs, err := util.GetTuples(j.b)
	if err != nil {
		return nil, err
	}
	mp, is, md := getMetadata(j.a, j.b)
	r := relation.New("", nil, md)
	for _, a := range as {
		for _, b := range bs {
			ok := true
			for _, i := range is {
				if value.Compare(a[i.a], b[i.b]) != 0 {
					ok = false
					break
				}
			}
			if ok {
				r.AddTuple(append(a, getTuple(b, mp)...))
			}
		}
	}
	return r, nil
}

type index struct {
	a, b int
}

func getTuple(t value.Tuple, mp map[int]struct{}) value.Tuple {
	var r value.Tuple

	for i, j := 0, len(t); i < j; i++ {
		if _, ok := mp[i]; !ok {
			r = append(r, t[i])
		}
	}
	return r
}

func getMetadata(a, b relation.Relation) (map[int]struct{}, []*index, []*relation.AttributeMetadata) {
	var is []*index
	var rs []*relation.AttributeMetadata

	mp := make(map[string]int)
	as, bs := a.Metadata(), b.Metadata()
	for i, j := 0, len(as); i < j; i++ {
		mp[as[i].Name] = i
		as[i].Name = a.Name() + "." + as[i].Name
		rs = append(rs, &relation.AttributeMetadata{as[i].Name, make(map[int32]int)})
	}
	mq := make(map[int]struct{})
	for i, j := 0, len(bs); i < j; i++ {
		if idx, ok := mp[bs[i].Name]; !ok {
			bs[i].Name = b.Name() + "." + bs[i].Name
			rs = append(rs, &relation.AttributeMetadata{bs[i].Name, make(map[int32]int)})
		} else {
			mq[i] = struct{}{}
			is = append(is, &index{idx, i})
		}
	}
	return mq, is, rs
}
