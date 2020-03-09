package natural

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(c context.Context, a, b relation.Relation) *natural {
	return &natural{c, a, b}
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
	mp, is, attrs := getMetadata(j.a, j.b)
	r := mem.New("", attrs, j.c)
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

func getMetadata(a, b relation.Relation) (map[int]struct{}, []*index, []string) {
	var is []*index
	var rs []string

	mp := make(map[string]int)
	as, bs := a.Metadata(), b.Metadata()
	for i, j := 0, len(as); i < j; i++ {
		mp[as[i]] = i
		as[i] = a.Name() + "." + as[i]
		rs = append(rs, as[i])
	}
	mq := make(map[int]struct{})
	for i, j := 0, len(bs); i < j; i++ {
		if idx, ok := mp[bs[i]]; !ok {
			bs[i] = b.Name() + "." + bs[i]
			rs = append(rs, bs[i])
		} else {
			mq[i] = struct{}{}
			is = append(is, &index{idx, i})
		}
	}
	return mq, is, rs
}
