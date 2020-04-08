package intersect

import (
	"errors"
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(left, right op.OP, c context.Context) *intersect {
	if c.Less(right, left) {
		left, right = right, left
	}
	return &intersect{
		c:       c,
		left:    left,
		right:   right,
		isCheck: false,
	}
}

func (n *intersect) Size() float64 {
	return n.c.SetIntersectSize(n.left, n.right)
}

func (n *intersect) Cost() float64 {
	return n.c.SetIntersectCost(n.left, n.right)
}

func (n *intersect) Dup() op.OP {
	return &intersect{
		c:       n.c,
		left:    n.left,
		right:   n.right,
		isCheck: n.isCheck,
	}
}

func (n *intersect) Operate() int {
	return op.SetIntersect
}

func (n *intersect) Children() []op.OP {
	return []op.OP{n.left, n.right}
}

func (n *intersect) SetChild(o op.OP, idx int) {
	switch idx {
	case 0:
		n.left = o
	default:
		n.right = o
	}
}

func (n *intersect) IsOrdered() bool {
	return false
}

func (n *intersect) String() string {
	return fmt.Sprintf("(%s ∩  %s)", n.left, n.right)
}

func (n *intersect) Name() (string, error) {
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

func (n *intersect) AttributeList() ([]string, error) {
	return n.left.AttributeList()
}

func (n *intersect) GetTuples(limit int) (value.Array, error) {
	if !n.isCheck {
		if err := n.check(nil); err != nil {
			return nil, err
		}
		if ctr, err := n.c.NewCounter(); err != nil {
			return nil, err
		} else {
			n.ctr = ctr
		}
		if err := n.newByTuple(limit); err != nil {
			n.ctr.Destroy()
			return nil, err
		}
		n.isCheck = true
	}
	ts, err := n.right.GetTuples(limit)
	if err != nil {
		n.ctr.Destroy()
		return nil, err
	}
	if len(ts) == 0 {
		n.ctr.Destroy()
		return ts, nil
	}
	var a value.Array
	for i, j := 0, len(ts); i < j; i++ {
		if cnt, err := n.ctr.DecAndGet(ts[i]); err != nil {
			n.ctr.Destroy()
			return nil, err
		} else if cnt == 1 {
			a = append(a, ts[i])
		}
	}
	return a, nil
}

func (n *intersect) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		if ctr, err := n.c.NewCounter(); err != nil {
			return nil, err
		} else {
			n.ctr = ctr
		}
		if err := n.newByTuple(limit); err != nil {
			n.ctr.Destroy()
			return nil, err
		}
		n.isCheck = true
	}
	is, err := n.indexs(attrs)
	if err != nil {
		n.ctr.Destroy()
		return nil, err
	}
	ts, err := n.right.GetTuples(limit)
	if err != nil {
		n.ctr.Destroy()
		return nil, err
	}
	if len(ts) == 0 {
		n.ctr.Destroy()
		return nil, nil
	}
	rq := make(map[string]value.Array)
	for i, j := 0, len(ts); i < j; i++ {
		if cnt, err := n.ctr.DecAndGet(ts[i]); err != nil {
			n.ctr.Destroy()
			return nil, err
		} else if cnt == 1 {
			for idx, attr := range attrs {
				rq[attr] = append(rq[attr], ts[i].(value.Array)[is[idx]])
			}
		}
	}
	return rq, nil
}

func (n *intersect) newByTuple(limit int) error {
	for {
		ts, err := n.left.GetTuples(limit)
		if err != nil {
			return err
		}
		if len(ts) == 0 {
			return nil
		}
		for _, t := range ts {
			if err := n.ctr.Set(t); err != nil {
				return err
			}
		}
	}
}

func (n *intersect) indexs(attrs []string) ([]int, error) {
	as, err := n.left.AttributeList()
	if err != nil {
		return nil, err
	}
	return util.Indexs(attrs, as), nil
}

func (n *intersect) check(attrs []string) error {
	{
		lattrs, err := n.left.AttributeList()
		if err != nil {
			return err
		}
		rattrs, err := n.right.AttributeList()
		if err != nil {
			return err
		}
		if len(lattrs) != len(rattrs) {
			return errors.New("attribute not equal")
		}
		for i, j := 0, len(lattrs); i < j; i++ {
			if lattrs[i] != rattrs[i] {
				return errors.New("attribute not equal")
			}
		}
	}
	if len(attrs) == 0 {
		return nil
	}
	as, err := n.left.AttributeList()
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
