package mem

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
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

func (r *mem) Size() float64 {
	size := 0
	for _, t := range r.ts {
		size += t.Size()
	}
	return float64(size)
}

func (r *mem) Cost() float64 {
	return r.Size()
}

func (r *mem) Dup() op.OP {
	return &mem{
		ts:    r.ts,
		mp:    r.mp,
		name:  r.name,
		start: r.start,
		attrs: r.attrs,
	}
}

func (r *mem) Operate() int {
	return op.Relation
}

func (r *mem) Children() []op.OP {
	return nil
}

func (r *mem) SetChild(_ op.OP, _ int) {}
func (r *mem) IsOrdered() bool         { return false }

func (r *mem) Split(n int) ([]relation.Relation, error) {
	var rs []relation.Relation

	step := len(r.ts) / n
	if step < 1 {
		step = 1
	}
	for i, j := 0, len(r.ts); i < j; i += step {
		switch {
		case len(rs) == n-1:
			rs = append(rs, &mem{
				mp:    r.mp,
				name:  r.name,
				attrs: r.attrs,
				ts:    r.ts[i:],
			})
			return rs, nil
		default:
			cnt := step
			if cnt > j-i {
				cnt = j - i
			}
			rs = append(rs, &mem{
				mp:    r.mp,
				name:  r.name,
				attrs: r.attrs,
				ts:    r.ts[i : i+cnt],
			})
		}
	}
	return rs, nil
}

func (r *mem) String() string {
	return r.name
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
