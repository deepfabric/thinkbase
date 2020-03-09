package mem

import (
	"errors"
	"fmt"
	"sort"

	arelation "github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(name string, attrs []string, ct context.Context) *relation {
	mp := make(map[string]int)
	for i, attr := range attrs {
		mp[attr] = i
	}
	r := &relation{
		ct:    ct,
		mp:    mp,
		name:  name,
		attrs: attrs,
		plh:   ct.Placeholder(),
	}
	ct.AddRelation(r)
	return r
}

func (r *relation) Placeholder() int {
	return r.plh
}

func (r *relation) Limit(start, count int) (arelation.Relation, error) {
	if start < 0 || count <= 0 || start+count > len(r.tuple) {
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
		attrs: attrs,
		name:  r.name,
		ct:    r.ct,
		plh:   r.ct.Placeholder(),
		tuple: r.tuple[start : start+count],
	}
	rr.ct.AddRelation(rr)
	return rr, nil
}

func (r *relation) Split(n int) ([]arelation.Relation, error) {
	var rs []arelation.Relation

	step := len(r.tuple) / n
	if step < 1 {
		step = 1
	}
	for i, j := 0, len(r.tuple); i < j; i += step {
		cnt := step
		if cnt > j-i {
			cnt = j - i
		}
		attrs := make([]string, len(r.attrs))
		copy(attrs, r.attrs)
		mp := make(map[string]int)
		for i, attr := range attrs {
			mp[attr] = i
		}
		rs = append(rs, &relation{
			mp:    mp,
			attrs: attrs,
			name:  r.name,
			ct:    r.ct,
			plh:   r.ct.Placeholder(),
			tuple: r.tuple[i : i+cnt],
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

func (r *relation) Nub() error {
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

func (r *relation) AddTuple(t value.Tuple) error {
	r.tuple = append(r.tuple, t)
	return nil
}

func (r *relation) AddTuples(ts []value.Tuple) error {
	r.tuple = append(r.tuple, ts...)
	return nil
}

func (r *relation) GetTupleCount() (int, error) {
	return len(r.tuple), nil
}

func (r *relation) GetTuple(idx int) (value.Tuple, error) {
	if idx >= len(r.tuple) {
		return nil, errors.New("tuple not exist")
	}
	return r.tuple[idx], nil
}

func (r *relation) GetTuples(start, end int) ([]value.Tuple, error) {
	if start < 0 {
		start = 0
	}
	if end < 0 || end > len(r.tuple) {
		end = len(r.tuple)
	}
	return r.tuple[start:end], nil
}

func (r *relation) GetTuplesByIndex(is []int) ([]value.Tuple, error) {
	var ts []value.Tuple

	for _, i := range is {
		ts = append(ts, r.tuple[i])
	}
	return ts, nil
}

func (r *relation) GetAttributeIndex(name string) (int, error) {
	if idx, ok := r.mp[name]; ok {
		return idx, nil
	}
	return -1, fmt.Errorf("cannot find attribute '%s'", name)
}

func (r *relation) GetAttribute(name string) (value.Attribute, error) {
	var attr value.Attribute

	i, ok := r.mp[name]
	if !ok {
		return nil, fmt.Errorf("cannot find attribute '%s'", name)
	}
	for _, t := range r.tuple {
		attr = append(attr, t[i])
	}
	return attr, nil
}

func (r *relation) GetAttributeByLimit(name string, start, end int) (value.Attribute, error) {
	var attr value.Attribute

	idx, ok := r.mp[name]
	if !ok {
		return nil, fmt.Errorf("cannot find attribute '%s'", name)
	}
	if start < 0 {
		start = 0
	}
	if end < 0 || end > len(r.tuple) {
		end = len(r.tuple)
	}
	for i := start; i < end; i++ {
		attr = append(attr, r.tuple[i][idx])
	}
	return attr, nil
}

func (r *relation) Sort(attrs []string, descs []bool) error {
	ts := &tuples{descs, attrs, r, r.tuple}
	sort.Sort(ts)
	r.tuple = ts.tuple
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
