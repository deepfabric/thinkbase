package product

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(left, right op.OP, c context.Context) *product {
	if c.Less(right, left) {
		left, right = right, left
	}
	return &product{
		c:       c,
		left:    left,
		right:   right,
		isCheck: false,
	}
}

func (n *product) Size() float64 {
	return n.c.ProductSize(n.left, n.right)
}

func (n *product) Cost() float64 {
	return n.c.ProductCost(n.left, n.right)
}

func (n *product) Dup() op.OP {
	return &product{
		c:       n.c,
		left:    n.left,
		right:   n.right,
		isCheck: n.isCheck,
	}
}

func (n *product) Operate() int {
	return op.Product
}

func (n *product) Children() []op.OP {
	return []op.OP{n.left, n.right}
}

func (n *product) SetChild(o op.OP, idx int) {
	switch idx {
	case 0:
		n.left = o
	default:
		n.right = o
	}
}

func (n *product) IsOrdered() bool {
	return false
}

func (n *product) String() string {
	return fmt.Sprintf("(%s ⨯  %s)", n.left, n.right)
}

func (n *product) Name() (string, error) {
	ln, err := n.left.Name()
	if err != nil {
		return "", err
	}
	rn, err := n.right.Name()
	if err != nil {
		return "", err
	}
	return ln + "." + rn, nil
}

func (n *product) AttributeList() ([]string, error) {
	ln, err := n.left.Name()
	if err != nil {
		return nil, err
	}
	rn, err := n.right.Name()
	if err != nil {
		return nil, err
	}
	lattrs, err := n.left.AttributeList()
	if err != nil {
		return nil, err
	}
	rattrs, err := n.right.AttributeList()
	if err != nil {
		return nil, err
	}
	var rs []string
	mp := make(map[string]int)
	for i, attr := range rattrs {
		mp[attr] = i
		rs = append(rs, attr)
	}
	for _, attr := range lattrs {
		if i, ok := mp[attr]; !ok {
			rs = append(rs, attr)
		} else {
			rs[i] = rn + "." + rs[i]
			rs = append(rs, ln+"."+attr)
		}
	}
	return rs, nil
}

func (n *product) GetTuples(limit int) (value.Array, error) {
	if !n.isCheck {
		v, err := n.c.NewVector()
		if err != nil {
			return nil, err
		}
		n.v = v
		if err := n.newByTuple(); err != nil {
			n.v.Destroy()
			return nil, err
		}
		n.isCheck = true
	}
	ts, err := n.right.GetTuples(limit)
	if err != nil {
		n.v.Destroy()
		return nil, err
	}
	if len(ts) == 0 {
		n.v.Destroy()
		return nil, nil
	}
	var a value.Array
	length, err := n.v.Len()
	if err != nil {
		n.v.Destroy()
		return nil, err
	}
	for i, j := 0, len(ts); i < j; i++ {
		for k := 0; k < length; k++ {
			t, err := n.v.Get(k)
			if err != nil {
				n.v.Destroy()
				return nil, err
			}
			a = append(a, append(ts[i].(value.Array), t.(value.Array)...))
		}
	}
	return a, nil
}

func (n *product) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		v, err := n.c.NewVector()
		if err != nil {
			return nil, err
		}
		n.v = v
		if err := n.newByTuple(); err != nil {
			n.v.Destroy()
			return nil, err
		}
		n.isCheck = true
	}
	is, err := n.indexs(attrs)
	if err != nil {
		n.v.Destroy()
		return nil, err
	}
	ts, err := n.right.GetTuples(limit)
	if err != nil {
		n.v.Destroy()
		return nil, err
	}
	if len(ts) == 0 {
		n.v.Destroy()
		return nil, nil
	}
	length, err := n.v.Len()
	if err != nil {
		n.v.Destroy()
		return nil, err
	}
	rq := make(map[string]value.Array)
	for i, j := 0, len(ts); i < j; i++ {
		for k := 0; k < length; k++ {
			t, err := n.v.Get(k)
			if err != nil {
				n.v.Destroy()
				return nil, err
			}
			a := append(ts[i].(value.Array), t.(value.Array)...)
			for idx, attr := range attrs {
				rq[attr] = append(rq[attr], a[is[idx]])
			}
		}
	}
	return rq, nil
}

func (n *product) newByTuple() error {
	limit := n.c.MemSize()
	for {
		ts, err := n.left.GetTuples(limit)
		if err != nil {
			return err
		}
		if len(ts) == 0 {
			return nil
		}
		if err := n.v.Append(ts); err != nil {
			return err
		}
	}
}

func (n *product) indexs(attrs []string) ([]int, error) {
	as, err := n.AttributeList()
	if err != nil {
		return nil, err
	}
	return util.Indexs(attrs, as), nil
}

func (n *product) check(attrs []string) error {
	if len(attrs) == 0 {
		return nil
	}
	as, err := n.AttributeList()
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
