package mem

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(name string, attrs []string) *mem {
	mp := make(map[string]int)
	for i, attr := range attrs {
		mp[attr] = i
	}
	return &mem{
		mp:    mp,
		name:  name,
		attrs: attrs,
	}
}

func (r *mem) Destroy() error {
	return nil
}

func (r *mem) Size() (int, error) {
	return 0, nil
}

func (r *mem) Split(n int) ([]relation.Relation, error) {
	return []relation.Relation{r}, nil
}

func (r *mem) Name() (string, error) {
	return r.name, nil
}

func (r *mem) AttributeList() ([]string, error) {
	return r.attrs, nil
}

func (r *mem) AddTuples(ts []value.Array) error {
	r.ts = append(r.ts, ts...)
	return nil
}

func (r *mem) GetTuples(limit int) (value.Array, error) {
	var rs value.Array

	for size := 0; size < limit && r.start < len(r.ts); r.start++ {
		size += r.ts[r.start].Size()
		rs = append(rs, r.ts[r.start])
	}
	return rs, nil
}

func (r *mem) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	rq := make(map[string]value.Array)
	for size := 0; size < limit && r.start < len(r.ts); r.start++ {
		for _, attr := range attrs {
			v := r.ts[r.start][r.mp[attr]]
			size += v.Size()
			rq[attr] = append(rq[attr], v)
		}
	}
	return rq, nil
}
