package order

import (
	"fmt"
	"sort"

	"github.com/deepfabric/thinkbase/pkg/vm/container/vector"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(prev op.OP, descs []bool, attrs []string, c context.Context) *order {
	return &order{
		c:       c,
		prev:    prev,
		isCheck: false,
		descs:   descs,
		attrs:   attrs,
	}
}

func (n *order) Name() (string, error) {
	return n.prev.Name()
}

func (n *order) AttributeList() ([]string, error) {
	return n.prev.AttributeList()
}

func (n *order) GetTuples(limit int) (value.Array, error) {
	attrs, err := n.prev.AttributeList()
	if err != nil {
		return nil, err
	}
	lt := n.newLt(attrs)
	if !n.isCheck {
		if err := n.check(n.attrs); err != nil {
			return nil, err
		}
		if err := n.newByTuple(lt); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	var size int
	var a value.Array
	for {
		if size >= limit {
			break
		}
		idx, end, err := n.findMin(lt)
		if err != nil {
			return nil, err
		}
		if end {
			return a, nil
		}
		v, err := n.vs[idx].Pop()
		if err != nil {
			return nil, err
		}
		size += v.Size()
		a = append(a, v)
	}
	return a, nil
}

func (n *order) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	var as [][]string

	as = append(as, util.MergeAttributes(attrs, n.attrs))
	lt := n.newLt(as[0])
	if !n.isCheck {
		if err := n.check(as[0]); err != nil {
			return nil, err
		}
		if err := n.newByAttributes(as[0], lt); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	size := 0
	rq := make(map[string]value.Array)
	for {
		if size >= limit {
			break
		}
		idx, end, err := n.findMin(lt)
		if err != nil {
			return nil, err
		}
		if end {
			return rq, nil
		}
		v, err := n.vs[idx].Pop()
		if err != nil {
			return nil, err
		}
		size += v.Size()
		mp := util.Tuple2Map(v.(value.Array), as[0])
		for _, attr := range attrs {
			rq[attr] = append(rq[attr], mp[attr])
		}
	}
	return rq, nil
}

func (n *order) newByTuple(lt func(value.Value, value.Value) bool) error {
	limit := n.c.MemSize()
	for {
		ts, err := n.prev.GetTuples(limit)
		if err != nil {
			return err
		}
		if len(ts) == 0 {
			return nil
		}
		sort.Sort(&tuples{ts, lt})
		v, err := n.c.NewVector()
		if err != nil {
			return err
		}
		if err := v.Append(ts); err != nil {
			return err
		}
		n.vs = append(n.vs, v)
	}
	return nil
}

func (n *order) newByAttributes(attrs []string, lt func(value.Value, value.Value) bool) error {
	limit := n.c.MemSize()
	for {
		mp, err := n.prev.GetAttributes(attrs, limit)
		if err != nil {
			return err
		}
		if len(mp) == 0 || len(mp[attrs[0]]) == 0 {
			return nil
		}
		ts := util.Map2Tuples(mp, attrs)
		sort.Sort(&tuples{ts, lt})
		v, err := n.c.NewVector()
		if err != nil {
			return err
		}
		v.Append(ts)
		n.vs = append(n.vs, v)
	}
	return nil
}

func (n *order) check(attrs []string) error {
	as, err := n.prev.AttributeList()
	if err != nil {
		return err
	}
	mp := make(map[string]struct{})
	for _, a := range as {
		mp[a] = struct{}{}
	}
	for _, attr := range attrs {
		if _, ok := mp[attr]; !ok {
			return fmt.Errorf("failed to find attribute '%s'", attr)
		}
	}
	return nil
}

func (n *order) findMin(lt func(value.Value, value.Value) bool) (int, bool, error) {
	vs, err := cleanup(n.vs)
	if err != nil {
		return -1, false, err
	}
	n.vs = vs
	if len(vs) == 0 {
		return -1, true, nil
	}
	idx := 0
	min, err := n.vs[0].Head()
	if err != nil {
		return -1, false, err
	}
	for i, j := 1, len(n.vs); i < j; i++ {
		v, err := n.vs[i].Head()
		if err != nil {
			return -1, false, nil
		}
		if lt(v, min) {
			idx = i
		}
	}
	return idx, false, nil
}

func (n *order) newLt(attrs []string) func(value.Value, value.Value) bool {
	var is []int // is表示需要排序的属性在属性中的位置

	mp := n.attributeIndex(attrs)
	for _, attr := range n.attrs {
		is = append(is, mp[attr])
	}
	return func(x, y value.Value) bool {
		xs, ys := x.(value.Array), y.(value.Array)
		for _, i := range is {
			if r := int(xs[i].ResolvedType().Oid - ys[i].ResolvedType().Oid); r != 0 {
				if n.descs[i] {
					return r > 0
				}
				return r < 0
			}
			if r := xs[i].Compare(ys[i]); r != 0 {
				if n.descs[i] {
					return r > 0
				}
				return r < 0
			}
		}
		return false
	}
}

func (n *order) attributeIndex(attrs []string) map[string]int {
	mp := make(map[string]int)
	for i, attr := range attrs {
		mp[attr] = i
	}
	return mp
}

func cleanup(vs []vector.Vector) ([]vector.Vector, error) {
	if len(vs) == 0 {
		return vs, nil
	}
	if ok, err := vs[0].IsEmpty(); err != nil {
		return nil, err
	} else if ok {
		vs[0].Destroy()
		return cleanup(vs[1:])
	}
	tail, err := cleanup(vs[1:])
	if err != nil {
		return nil, err
	}
	return append(vs[:1], tail...), nil
}
