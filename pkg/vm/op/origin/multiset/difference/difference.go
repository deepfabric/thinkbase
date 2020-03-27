package difference

import (
	"errors"
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(left, right op.OP, c context.Context) *difference {
	return &difference{
		c:         c,
		left:      left,
		right:     right,
		isCheck:   false,
		isLeftMin: c.Less(left, right),
	}
}

func (n *difference) AttributeList() ([]string, error) {
	return n.left.AttributeList()
}

func (n *difference) GetTuples(limit int) (value.Array, error) {
	if n.isLeftMin {
		return n.getTuplesByLeft(limit)
	}
	return n.getTuplesByRight(limit)
}

func (n *difference) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	if n.isLeftMin {
		return n.getAttributesByLeft(attrs, limit)
	}
	return n.getAttributesByRight(attrs, limit)
}

func (n *difference) getTuplesByLeft(limit int) (value.Array, error) {
	if !n.isCheck {
		if ctr, err := n.c.NewCounter(); err != nil {
			return nil, err
		} else {
			n.ctr = ctr
		}
		if err := n.newByLeft(limit); err != nil {
			n.ctr.Destroy()
			return nil, err
		}
		n.isCheck = true
	}
	ts, err := n.ctr.Pops(limit)
	if err != nil {
		n.ctr.Destroy()
		return nil, err
	}
	if len(ts) == 0 {
		n.ctr.Destroy()
		return nil, nil
	}
	return ts, nil
}

func (n *difference) getTuplesByRight(limit int) (value.Array, error) {
	if !n.isCheck {
		if ctr, err := n.c.NewCounter(); err != nil {
			return nil, err
		} else {
			n.ctr = ctr
		}
		if err := n.newByRight(limit); err != nil {
			n.ctr.Destroy()
			return nil, err
		}
		n.isCheck = true
	}
	ts, err := n.left.GetTuples(limit)
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
		} else if cnt == 0 {
			a = append(a, ts[i])
		}
	}
	return a, nil
}

func (n *difference) getAttributesByLeft(attrs []string, limit int) (map[string]value.Array, error) {
	if !n.isCheck {
		if ctr, err := n.c.NewCounter(); err != nil {
			return nil, err
		} else {
			n.ctr = ctr
		}
		if err := n.newByLeft(limit); err != nil {
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
	ts, err := n.ctr.Pops(limit)
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
		for idx, attr := range attrs {
			rq[attr] = append(rq[attr], ts[i].(value.Array)[is[idx]])
		}
	}
	return rq, nil
}

func (n *difference) getAttributesByRight(attrs []string, limit int) (map[string]value.Array, error) {
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		if ctr, err := n.c.NewCounter(); err != nil {
			return nil, err
		} else {
			n.ctr = ctr
		}
		if err := n.newByRight(limit); err != nil {
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
	ts, err := n.left.GetTuples(limit)
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
		} else if cnt == 0 {
			for idx, attr := range attrs {
				rq[attr] = append(rq[attr], ts[i].(value.Array)[is[idx]])
			}
		}
	}
	return rq, nil
}

func (n *difference) newByLeft(limit int) error {
	for {
		ts, err := n.left.GetTuples(limit)
		if err != nil {
			return err
		}
		if len(ts) == 0 {
			break
		}
		for _, t := range ts {
			if err := n.ctr.Inc(t); err != nil {
				return err
			}
		}
	}
	for {
		ts, err := n.right.GetTuples(limit)
		if err != nil {
			return err
		}
		if len(ts) == 0 {
			return nil
		}
		for _, t := range ts {
			if _, err := n.ctr.Dec(t); err != nil {
				return err
			}
		}
	}
}

func (n *difference) newByRight(limit int) error {
	for {
		ts, err := n.right.GetTuples(limit)
		if err != nil {
			return err
		}
		if len(ts) == 0 {
			return nil
		}
		for _, t := range ts {
			if err := n.ctr.Inc(t); err != nil {
				return err
			}
		}
	}
}

func (n *difference) indexs(attrs []string) ([]int, error) {
	as, err := n.left.AttributeList()
	if err != nil {
		return nil, err
	}
	return util.Indexs(attrs, as), nil
}

func (n *difference) check(attrs []string) error {
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
