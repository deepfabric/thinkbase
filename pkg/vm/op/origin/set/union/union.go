package union

import (
	"errors"
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	hunion "github.com/deepfabric/thinkbase/pkg/vm/op/parallel/tail/local/set/hash/union"
	ounion "github.com/deepfabric/thinkbase/pkg/vm/op/parallel/tail/local/set/order/union"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(left, right op.OP, c context.Context) *union {
	if c.Less(right, left) {
		left, right = right, left
	}
	return &union{
		c:           c,
		left:        left,
		right:       right,
		isCheck:     false,
		isLeftEmpty: false,
	}
}

func (n *union) NewHashUnion(left, right op.OP) op.OP {
	return hunion.New(left, right, n.c)
}

func (n *union) NewOrderUnion(left, right op.OP) op.OP {
	return ounion.New(left, right, n.c)
}

func (n *union) Size() float64 {
	return n.c.SetUnionSize(n.left, n.right)
}

func (n *union) Cost() float64 {
	return n.c.SetUnionCost(n.left, n.right)
}

func (n *union) Dup() op.OP {
	return &union{
		c:           n.c,
		left:        n.left,
		right:       n.right,
		isCheck:     n.isCheck,
		isLeftEmpty: n.isLeftEmpty,
	}
}

func (n *union) Operate() int {
	return op.SetUnion
}

func (n *union) Children() []op.OP {
	return []op.OP{n.left, n.right}
}

func (n *union) SetChild(o op.OP, idx int) {
	switch idx {
	case 0:
		n.left = o
	default:
		n.right = o
	}
}

func (n *union) IsOrdered() bool {
	return false
}

func (n *union) String() string {
	return fmt.Sprintf("(%s ∪  %s)", n.left, n.right)
}

func (n *union) Name() (string, error) {
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

func (n *union) AttributeList() ([]string, error) {
	return n.left.AttributeList()
}

func (n *union) GetTuples(limit int) (value.Array, error) {
	if !n.isCheck {
		if err := n.check(nil); err != nil {
			return nil, err
		}
		if dict, err := n.c.NewDictionary(); err != nil {
			return nil, err
		} else {
			n.dict = dict
		}
		n.isCheck = true
	}
	if !n.isLeftEmpty {
		ts, err := n.left.GetTuples(limit)
		if err != nil {
			n.dict.Destroy()
			return nil, err
		}
		if len(ts) > 0 {
			var a value.Array
			for i, j := 0, len(ts); i < j; i++ {
				if ok, _, err := n.dict.GetOrSet(ts[i], nil); err != nil {
					n.dict.Destroy()
					return nil, err
				} else if !ok {
					a = append(a, ts[i])
				}
			}
			return a, nil
		}
		n.isLeftEmpty = true
	}
	ts, err := n.right.GetTuples(limit)
	if err != nil {
		n.dict.Destroy()
		return nil, err
	}
	if len(ts) == 0 {
		n.dict.Destroy()
		return ts, nil
	}
	var a value.Array
	for i, j := 0, len(ts); i < j; i++ {
		if ok, _, err := n.dict.GetOrSet(ts[i], nil); err != nil {
			n.dict.Destroy()
			return nil, err
		} else if !ok {
			a = append(a, ts[i])
		}
	}
	return a, nil
}

func (n *union) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		if dict, err := n.c.NewDictionary(); err != nil {
			return nil, err
		} else {
			n.dict = dict
		}
		n.isCheck = true
	}
	is, err := n.indexs(attrs)
	if err != nil {
		n.dict.Destroy()
		return nil, err
	}
	if !n.isLeftEmpty {
		ts, err := n.left.GetTuples(limit)
		if err != nil {
			n.dict.Destroy()
			return nil, err
		}
		if len(ts) > 0 {
			rq := make(map[string]value.Array)
			for i, j := 0, len(ts); i < j; i++ {
				if ok, _, err := n.dict.GetOrSet(ts[i], nil); err != nil {
					n.dict.Destroy()
					return nil, err
				} else if !ok {
					for idx, attr := range attrs {
						rq[attr] = append(rq[attr], ts[i].(value.Array)[is[idx]])
					}
				}
			}
			return rq, nil
		}
		n.isLeftEmpty = true
	}
	ts, err := n.right.GetTuples(limit)
	if err != nil {
		n.dict.Destroy()
		return nil, err
	}
	if len(ts) == 0 {
		n.dict.Destroy()
		return nil, nil
	}
	rq := make(map[string]value.Array)
	for i, j := 0, len(ts); i < j; i++ {
		if ok, _, err := n.dict.GetOrSet(ts[i], nil); err != nil {
			n.dict.Destroy()
			return nil, err
		} else if !ok {
			for idx, attr := range attrs {
				rq[attr] = append(rq[attr], ts[i].(value.Array)[is[idx]])
			}
		}
	}
	return rq, nil
}

func (n *union) indexs(attrs []string) ([]int, error) {
	as, err := n.left.AttributeList()
	if err != nil {
		return nil, err
	}
	return util.Indexs(attrs, as), nil
}

func (n *union) check(attrs []string) error {
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
