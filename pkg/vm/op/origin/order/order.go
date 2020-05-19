package order

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/deepfabric/thinkbase/pkg/vm/container"
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

func (n *order) NewLt(attrs []string) func(value.Value, value.Value) bool {
	return n.newLt(attrs)
}

func (n *order) NewCmp(attrs []string) func(value.Value, value.Value) int {
	return n.newCmp(attrs)
}

func (n *order) Size() float64 {
	return n.c.OrderSize(n.prev, n.attrs)
}

func (n *order) Cost() float64 {
	return n.c.OrderCost(n.prev, n.attrs)
}

func (n *order) Dup() op.OP {
	return &order{
		c:       n.c,
		prev:    n.prev,
		descs:   n.descs,
		attrs:   n.attrs,
		isCheck: n.isCheck,
	}
}

func (n *order) SetChild(o op.OP, _ int) { n.prev = o }
func (n *order) IsOrdered() bool         { return true }
func (n *order) Operate() int            { return op.Order }
func (n *order) Children() []op.OP       { return []op.OP{n.prev} }

func (n *order) String() string {
	r := fmt.Sprintf("τ([")
	for i, attr := range n.attrs {
		switch i {
		case 0:
			r += fmt.Sprintf("%s", attr)
			if n.descs[i] {
				r += " desc"
			}
		default:
			r += fmt.Sprintf(", %s", attr)
			if n.descs[i] {
				r += " desc"
			}
		}
	}
	r += fmt.Sprintf("], %s)", n.prev)
	return r
}

func (n *order) Name() (string, error) {
	return n.prev.Name()
}

func (n *order) AttributeList() ([]string, error) {
	return n.prev.AttributeList()
}

func (n *order) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	var as [][]string

	attrs = util.MergeAttributes(attrs, []string{})
	as = append(as, util.MergeAttributes(attrs, n.attrs))
	lt := n.newLt(n.attrs)
	if !n.isCheck {
		if err := n.check(as[0]); err != nil {
			return nil, err
		}
		dv, err := n.c.NewDictVector()
		if err != nil {
			return nil, err
		}
		n.dv = dv
		if id, err := n.newByAttributes(as[0], lt, limit); err != nil {
			n.dv.Destroy()
			return nil, err
		} else {
			n.id = id
		}
		n.isCheck = true
	}
	ts, err := n.dv.Pops(n.id, limit)
	if err == container.NotExist || (err == nil && len(ts) == 0) {
		n.dv.Destroy()
		return nil, nil
	}
	if err != nil {
		n.dv.Destroy()
		return nil, err
	}
	rq := make(map[string]value.Array)
	mp := util.Tuples2Map(ts, as[0])
	for _, attr := range attrs {
		rq[attr] = mp[attr]
	}
	return rq, nil
}

func (n *order) newByAttributes(attrs []string, lt func(value.Value, value.Value) bool, limit int) (string, error) {
	id := strconv.FormatInt(int64(0), 10)
	mp, err := n.prev.GetAttributes(attrs, limit)
	if err != nil {
		return "", err
	}
	if len(mp) == 0 || len(mp[attrs[0]]) == 0 {
		return id, nil
	}
	ts := util.Map2Tuples(mp, attrs)
	sort.Sort(&tuples{ts, lt})
	if err := n.dv.Push(id, ts); err != nil {
		return "", err
	}
	for i := 1; ; i++ {
		mp, err := n.prev.GetAttributes(attrs, limit)
		if err != nil {
			return "", err
		}
		if len(mp) == 0 || len(mp[attrs[0]]) == 0 {
			break
		}
		ts := util.Map2Tuples(mp, attrs)
		sort.Sort(&tuples{ts, lt})
		nid := id + strconv.FormatInt(int64(i), 10)
		if err := n.merge(id, nid, limit, ts, lt); err != nil {
			return "", err
		}
		id = nid
	}
	return id, nil
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

func (n *order) merge(oid, nid string, limit int, ts value.Array, lt func(value.Value, value.Value) bool) error {
	var size int
	var r value.Array

	dv, err := n.c.NewDictVector()
	if err != nil {
		return err
	}
	a, err := n.dv.Pops(oid, limit)
	if err != nil && err != container.NotExist {
		dv.Destroy()
		return err
	}
	for len(a) > 0 && len(ts) > 0 {
		if lt(ts[0], a[0]) {
			size += ts[0].Size()
			r = append(r, ts[0])
			ts = ts[1:]
		} else {
			size += a[0].Size()
			r = append(r, a[0])
			if a = a[1:]; len(a) == 0 {
				if a, err = n.dv.Pops(oid, limit); err != nil && err != container.NotExist {
					dv.Destroy()
					return err
				}
			}
		}
		if size > limit {
			if err := dv.Push(nid, r); err != nil {
				dv.Destroy()
				return err
			}
			size = 0
			r = value.Array{}
		}
	}
	for len(a) > 0 {
		if err := dv.Push(nid, a); err != nil {
			dv.Destroy()
			return err
		}
		if a, err = n.dv.Pops(oid, limit); err != nil && err != container.NotExist {
			dv.Destroy()
			return err
		}
	}
	if len(ts) > 0 {
		if err := n.dv.Push(nid, ts); err != nil {
			return err
		}
	}
	n.dv.Destroy()
	n.dv = dv
	return nil
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
		return value.Compare(x, y) < 0
	}
}

func (n *order) newCmp(attrs []string) func(value.Value, value.Value) int {
	var is []int // is表示需要排序的属性在属性中的位置

	mp := n.attributeIndex(attrs)
	for _, attr := range n.attrs {
		is = append(is, mp[attr])
	}
	return func(x, y value.Value) int {
		xs, ys := x.(value.Array), y.(value.Array)
		for _, i := range is {
			if r := int(xs[i].ResolvedType().Oid - ys[i].ResolvedType().Oid); r != 0 {
				if n.descs[i] {
					return r * -1
				}
				return r
			}
			if r := xs[i].Compare(ys[i]); r != 0 {
				if n.descs[i] {
					return r * -1
				}
				return r
			}
		}
		return 0
	}
}

func (n *order) attributeIndex(attrs []string) map[string]int {
	mp := make(map[string]int)
	for i, attr := range attrs {
		mp[attr] = i
	}
	return mp
}
