package relation

import (
	"errors"
	"fmt"
	"sort"

	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
)

func New(name string, ref interface{}, attrs []*AttributeMetadata) *relation {
	return &relation{
		ref:      ref,
		name:     name,
		metadata: metadata{attrs},
	}
}

func (r *relation) Name() string {
	return r.name
}

func (r *relation) Reference() interface{} {
	return r.ref
}

func (r *relation) Metadata() []*AttributeMetadata {
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
	for i, j := 0, len(r.attrs); i < j; i++ {
		if r.attrs[i].Name == name {
			r.attrs[i].Name = alias
			return nil
		}
	}
	return fmt.Errorf("cannot find attribute '%s'", name)
}

func (r *relation) AddAttribute(attr *AttributeMetadata) error {
	r.attrs = append(r.attrs, attr)
	return nil
}

func (r *relation) AddTuple(t value.Tuple) error {
	r.addTuple(t)
	r.tuple = append(r.tuple, t)
	return nil
}

func (r *relation) AddTuples(ts []value.Tuple) error {
	for _, t := range ts {
		r.addTuple(t)
	}
	r.tuple = append(r.tuple, ts...)
	return nil
}

func (r *relation) GetTupleCount() (int, error) {
	if r.ref == nil {
		return len(r.tuple), nil
	}
	return -1, errors.New("unimplemented now")
}

func (r *relation) GetTuple(idx int) (value.Tuple, error) {
	if r.ref == nil {
		if idx >= len(r.tuple) {
			return nil, errors.New("tuple not exist")
		}
		return r.tuple[idx], nil
	}
	return nil, errors.New("unimplemented now")
}

func (r *relation) GetTuples(start, end int) ([]value.Tuple, error) {
	if r.ref == nil {
		if start < 0 {
			start = 0
		}
		if end < 0 || end > len(r.tuple) {
			end = len(r.tuple)
		}
		return r.tuple[start:end], nil
	}
	return nil, errors.New("unimplemented now")
}

func (r *relation) GetAttributeIndex(name string) (int, error) {
	if r.ref == nil {
		return r.getAttributeIndex(name), nil
	}
	return -1, errors.New("unimplemented now")
}

func (r *relation) GetAttribute(name string) (value.Attribute, error) {
	if r.ref == nil {
		var attr value.Attribute

		i := r.getAttributeIndex(name)
		if i < 0 {
			return nil, errors.New("attribute not exist")
		}
		for _, t := range r.tuple {
			attr = append(attr, t[i])
		}
		return attr, nil
	}
	return nil, errors.New("unimplemented now")
}

func (r *relation) GetAttributeByLimit(name string, start, end int) (value.Attribute, error) {
	if r.ref == nil {
		var attr value.Attribute

		idx := r.getAttributeIndex(name)
		if idx < 0 {
			return nil, errors.New("attribute not exist")
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
	return nil, errors.New("unimplemented now")
}

func (r *relation) Sort(attrs []string, descs []bool) error {
	if r.ref == nil {
		ts := &tuples{descs, attrs, r, r.tuple}
		sort.Sort(ts)
		r.tuple = ts.tuple
		return nil
	}
	return errors.New("unimplemented now")
}

func (r *relation) getAttributeIndex(name string) int {
	for i, a := range r.attrs {
		if a.Name == name {
			return i
		}
	}
	return -1
}

func (r *relation) addTuple(t value.Tuple) error {
	for i, v := range t {
		oid := v.ResolvedType().Oid
		if cnt, ok := r.attrs[i].Types[oid]; !ok {
			r.attrs[i].Types[oid] = 1
		} else {
			r.attrs[i].Types[oid] = cnt + 1
		}
	}
	return nil
}

func (r *relation) less(tx, ty value.Tuple, attrs []string, descs []bool) bool {
	for idx, attr := range attrs {
		if i := r.getAttributeIndex(attr); i >= 0 {
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
