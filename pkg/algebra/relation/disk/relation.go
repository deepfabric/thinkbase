package disk

import (
	"errors"
	"fmt"
	"sort"

	arelation "github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/storage"
)

func New(id string, db storage.Database) (*relation, error) {
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
	return &relation{
		mp:    mp,
		name:  id,
		tbl:   tbl,
		attrs: attrs,
		md:    newMetadata(cnt, 0, id, attrs),
	}, nil
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
			md:    newMetadata(cnt, i, r.md.id, r.md.attrs),
		})
	}
	return rs, nil
}

func (r *relation) Name() string {
	return r.name
}

func (r *relation) Metadata() []string {
	return r.attrs
}

func (r *relation) Nub() error {
	if err := r.load(); err != nil {
		return err
	}
	if len(r.tuple) > 1 {
		for i, j := 0, len(r.tuple); i < j; i++ {
			remove(i+1, r.tuple[i], &r.tuple)
			j = len(r.tuple)
		}
	}
	return nil
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

func (r *relation) GetAttributeIndex(name string) (int, error) {
	if idx, ok := r.mp[name]; ok {
		return idx, nil
	}
	return -1, fmt.Errorf("cannot find attribute '%s'", name)
}

func (r *relation) GetAttribute(name string) (value.Attribute, error) {
	return r.tbl.GetAttributeByLimit(name, r.md.start, r.md.start+r.md.cnt)
}

func (r *relation) GetAttributeByLimit(name string, start, end int) (value.Attribute, error) {
	return r.tbl.GetAttributeByLimit(name, r.md.start+start, r.md.start+end)
}

func (r *relation) Sort(attrs []string, descs []bool) error {
	if err := r.load(); err != nil {
		return err
	}
	ts := &tuples{descs, attrs, r, r.tuple}
	sort.Sort(ts)
	r.tuple = ts.tuple
	return nil
}

func (r *relation) load() error {
	if len(r.tuple) >= r.md.cnt {
		return nil
	}
	if ts, err := r.tbl.GetTuples(r.md.start, r.md.start+r.md.cnt, r.md.attrs); err != nil {
		return err
	} else {
		r.tuple = ts
	}
	return nil
}

func (r *relation) less(tx, ty value.Tuple, attrs []string, descs []bool) bool {
	for idx, attr := range attrs {
		if i, ok := r.mp[attr]; ok {
			if r := int(tx[i].ResolvedType().Oid - ty[i].ResolvedType().Oid); r != 0 {
				return less(descs[idx], r)
			}
			if r := tx[i].Compare(ty[i]); r != 0 {
				return less(descs[idx], r)
			}
		}
	}
	return false
}

func remove(start int, x value.Tuple, xs *[]value.Tuple) {
	for i, j := start, len(*xs); i < j; i++ {
		if x.Compare(((*xs)[i])) == 0 {
			*xs = append((*xs)[:i], (*xs)[i+1:]...)
			i--
			j = len(*xs)
		}
	}
}

func less(desc bool, r int) bool {
	if desc {
		return r > 0
	}
	return r < 0
}

func newMetadata(cnt int, start int, id string, attrs []string) *metadata {
	as := make([]string, len(attrs))
	copy(as, attrs)
	return &metadata{cnt, start, id, attrs}
}
