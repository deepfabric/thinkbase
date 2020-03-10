package disk

import (
	"errors"
	"fmt"

	arelation "github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/storage"
)

func New(id string, db storage.Database, ct context.Context) (*relation, error) {
	tbl, err := db.Table(id)
	if err != nil {
		return nil, err
	}
	cnt, err := tbl.GetTupleCount()
	if err != nil {
		return nil, err
	}
	attrs := tbl.Metadata()
	mp := make(map[string]int)
	for i, attr := range attrs {
		mp[attr] = i
	}
	r := &relation{
		ct:    ct,
		mp:    mp,
		name:  id,
		tbl:   tbl,
		attrs: attrs,
		plh:   ct.Placeholder(),
		md:    newMetadata(cnt, 0, id, attrs),
		amp:   make(map[string]value.Attribute),
	}
	ct.AddRelation(r)
	return r, nil
}

func (r *relation) Placeholder() int {
	return r.plh
}

func (r *relation) Limit(start, count int) (arelation.Relation, error) {
	if start < 0 || count <= 0 || start+count > r.md.cnt {
		return nil, errors.New("out of size")
	}
	attrs := make([]string, len(r.attrs))
	copy(attrs, r.attrs)
	mp := make(map[string]int)
	for i, attr := range attrs {
		mp[attr] = i
	}
	rr := &relation{
		mp:    mp,
		tbl:   r.tbl,
		attrs: attrs,
		name:  r.name,
		ct:    r.ct,
		plh:   r.ct.Placeholder(),
		amp:   make(map[string]value.Attribute),
		md:    newMetadata(count, start, r.md.id, r.md.attrs),
	}
	r.ct.AddRelation(rr)
	return rr, nil
}

func (r *relation) Split(n int) ([]arelation.Relation, error) {
	var rs []arelation.Relation

	step := r.md.cnt / n
	if step < 1 {
		step = 1
	}
	for i := 0; i < r.md.cnt; i += step {
		cnt := step
		if cnt > r.md.cnt-i {
			cnt = r.md.cnt - i
		}
		attrs := make([]string, len(r.attrs))
		copy(attrs, r.attrs)
		mp := make(map[string]int)
		for i, attr := range attrs {
			mp[attr] = i
		}
		rs = append(rs, &relation{
			mp:    mp,
			tbl:   r.tbl,
			attrs: attrs,
			name:  r.name,
			ct:    r.ct,
			plh:   r.ct.Placeholder(),
			amp:   make(map[string]value.Attribute),
			md:    newMetadata(cnt, i, r.md.id, r.md.attrs),
		})
		r.ct.AddRelation(rs[len(rs)-1])
	}
	return rs, nil
}

func (r *relation) Name() string {
	return r.name
}

func (r *relation) Metadata() []string {
	return r.attrs
}

func (r *relation) Rename(name string) error {
	r.name = name
	return nil
}

func (r *relation) RenameAttribute(name, alias string) error {
	if _, ok := r.mp[alias]; ok {
		return fmt.Errorf("attribute '%s' is exist", alias)
	}
	for i, j := 0, len(r.attrs); i < j; i++ {
		if r.attrs[i] == name {
			r.attrs[i] = alias
			r.mp[alias] = i
			delete(r.mp, name)
			return nil
		}
	}
	return fmt.Errorf("cannot find attribute '%s'", name)
}

func (r *relation) AddTuple(_ value.Tuple) error {
	return errors.New("illegal operation")
}

func (r *relation) AddTuples(_ []value.Tuple) error {
	return errors.New("illegal operation")
}

func (r *relation) GetTupleCount() (int, error) {
	return r.md.cnt, nil
}

func (r *relation) GetTuple(idx int) (value.Tuple, error) {
	return r.tbl.GetTuple(r.md.start+idx, r.md.attrs)
}

func (r *relation) GetTuples(start, end int) ([]value.Tuple, error) {
	return r.tbl.GetTuples(r.md.start+start, r.md.start+end, r.md.attrs)
}

func (r *relation) GetTuplesByIndex(is []int) ([]value.Tuple, error) {
	return r.tbl.GetTuplesByIndex(is, r.md.attrs)
}

func (r *relation) GetAttributeIndex(name string) (int, error) {
	if idx, ok := r.mp[name]; ok {
		return idx, nil
	}
	return -1, fmt.Errorf("cannot find attribute '%s'", name)
}

func (r *relation) GetAttribute(name string) (value.Attribute, error) {
	if a, ok := r.amp[name]; ok {
		return a, nil
	}
	a, err := r.tbl.GetAttributeByLimit(name, r.md.start, r.md.start+r.md.cnt)
	if err != nil {
		return nil, err
	}
	r.amp[name] = a
	return a, nil
}

func (r *relation) GetAttributeByLimit(name string, start, end int) (value.Attribute, error) {
	if a, ok := r.amp[name]; ok {
		if start < 0 {
			start = 0
		}
		if end < 0 || end > r.md.cnt {
			end = r.md.cnt
		}
		return a[start:end], nil
	}
	return r.tbl.GetAttributeByLimit(name, r.md.start+start, r.md.start+end)
}

func newMetadata(cnt int, start int, id string, attrs []string) *metadata {
	as := make([]string, len(attrs))
	copy(as, attrs)
	return &metadata{cnt, start, id, attrs}
}
