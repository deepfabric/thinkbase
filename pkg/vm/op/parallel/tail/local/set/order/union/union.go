package union

import (
	"errors"
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(r, s op.OP, c context.Context) *union {
	return &union{
		c:        c,
		r:        r,
		s:        s,
		isCheck:  false,
		isREmpty: false,
		isSEmpty: false,
		lt:       r.(op.OrderOP).NewLT(),
	}
}

func (n *union) Size() float64 {
	return n.c.SetUnionSizeByOrder(n.r, n.s)
}

func (n *union) Cost() float64 {
	return n.c.SetUnionCostByOrder(n.r, n.s)
}

func (n *union) Dup() op.OP {
	return &union{
		c:        n.c,
		r:        n.r,
		s:        n.s,
		lt:       n.lt,
		isCheck:  n.isCheck,
		isREmpty: n.isREmpty,
		isSEmpty: n.isSEmpty,
	}
}

func (n *union) Operate() int {
	return op.SetUnion
}

func (n *union) Children() []op.OP {
	return []op.OP{n.r, n.s}
}

func (n *union) SetChild(o op.OP, idx int) {
	switch idx {
	case 0:
		n.r = o
	default:
		n.s = o
	}
}

func (n *union) IsOrdered() bool {
	return false
}

func (n *union) String() string {
	return fmt.Sprintf("(%s ∪  %s, order union)", n.r, n.s)
}

func (n *union) Name() (string, error) {
	rn, err := n.r.Name()
	if err != nil {
		return "", err
	}
	sn, err := n.s.Name()
	if err != nil {
		return "", err
	}
	return rn + "." + sn, nil
}

func (n *union) AttributeList() ([]string, error) {
	return n.r.AttributeList()
}

func (n *union) GetTuples(limit int) (value.Array, error) {
	if !n.isCheck {
		if err := n.check(nil); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	if !n.isREmpty && len(n.rts) == 0 {
		if err := n.fillR(limit / 2); err != nil {
			return nil, err
		}
	}
	if !n.isSEmpty && len(n.sts) == 0 {
		if err := n.fillS(limit / 2); err != nil {
			return nil, err
		}
	}
	if n.isREmpty && n.isSEmpty {
		return nil, nil
	}
	var size int
	var a value.Array
	for {
		if size >= limit {
			break
		}
		t, end, err := n.findMin(limit)
		if err != nil {
			return nil, err
		}
		if end {
			return a, nil
		}
		size += t.Size()
		a = append(a, t)
	}
	return a, nil
}

func (n *union) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	as, err := n.AttributeList()
	if err != nil {
		return nil, err
	}
	ts, err := n.GetTuples(limit)
	if err != nil {
		return nil, err
	}
	mp := util.Tuples2Map(ts, as)
	rq := make(map[string]value.Array)
	for _, attr := range attrs {
		rq[attr] = append(rq[attr], mp[attr]...)
	}
	return rq, nil
}

func (n *union) fillR(limit int) error {
	ts, err := n.r.GetTuples(limit)
	if err != nil {
		return err
	}
	if n.rts = ts; len(n.rts) == 0 {
		n.isREmpty = true
	}
	return nil
}

func (n *union) fillS(limit int) error {
	ts, err := n.s.GetTuples(limit)
	if err != nil {
		return err
	}
	if n.sts = ts; len(n.sts) == 0 {
		n.isSEmpty = true
	}
	return nil
}

func (n *union) findMin(limit int) (value.Value, bool, error) {
	switch {
	case n.isREmpty:
		for {
			if n.isSEmpty {
				return nil, true, nil
			}
			if len(n.sts) > 0 {
				t := n.sts[0]
				n.sts = n.removeTuple(n.sts, t)
				return t, false, nil
			}
			if err := n.fillS(limit); err != nil {
				return nil, false, err
			}
		}
	case n.isSEmpty:
		for {
			if n.isREmpty {
				return nil, true, nil
			}
			if len(n.rts) > 0 {
				t := n.rts[0]
				n.rts = n.removeTuple(n.rts, t)
				return t, false, nil
			}
			if err := n.fillR(limit); err != nil {
				return nil, false, err
			}
		}
	}
	t := n.rts[0]
	if n.lt(n.sts[0], n.rts[0]) {
		t = n.sts[0]
	}
	for {
		if n.isREmpty {
			break
		}
		if n.rts = n.removeTuple(n.rts, t); len(n.rts) > 0 {
			break
		}
		if err := n.fillR(limit / 2); err != nil {
			return nil, false, err
		}
	}
	for {
		if n.isSEmpty {
			break
		}
		if n.sts = n.removeTuple(n.sts, t); len(n.sts) > 0 {
			break
		}
		if err := n.fillS(limit / 2); err != nil {
			return nil, false, err
		}
	}
	return t, false, nil
}

func (n *union) removeTuple(ts value.Array, t value.Value) value.Array {
	var i, j int

	for i, j = 0, len(ts); i < j; i++ {
		if value.Compare(t, ts[i]) != 0 {
			break
		}
	}
	return ts[i:]
}

func (n *union) check(attrs []string) error {
	{
		rattrs, err := n.r.AttributeList()
		if err != nil {
			return err
		}
		sattrs, err := n.s.AttributeList()
		if err != nil {
			return err
		}
		if len(rattrs) != len(sattrs) {
			return errors.New("attribute not equal")
		}
		for i, j := 0, len(rattrs); i < j; i++ {
			if rattrs[i] != sattrs[i] {
				return errors.New("attribute not equal")
			}
		}
	}
	if len(attrs) == 0 {
		return nil
	}
	as, err := n.r.AttributeList()
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
