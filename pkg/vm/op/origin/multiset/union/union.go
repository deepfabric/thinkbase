package union

import (
	"errors"
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(left, right op.OP, c context.Context) *union {
	return &union{
		c:           c,
		left:        left,
		right:       right,
		isCheck:     false,
		isLeftEmpty: false,
	}
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
		n.isCheck = true
	}
	if !n.isLeftEmpty {
		ts, err := n.left.GetTuples(limit)
		if err != nil {
			return nil, err
		}
		if len(ts) > 0 {
			return ts, nil
		}
		n.isLeftEmpty = true
	}
	return n.right.GetTuples(limit)
}

func (n *union) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	is, err := n.indexs(attrs)
	if err != nil {
		return nil, err
	}
	if !n.isLeftEmpty {
		ts, err := n.left.GetTuples(limit)
		if err != nil {
			return nil, err
		}
		if len(ts) > 0 {
			rq := make(map[string]value.Array)
			for i, j := 0, len(ts); i < j; i++ {
				for idx, attr := range attrs {
					rq[attr] = append(rq[attr], ts[i].(value.Array)[is[idx]])
				}
			}
			return rq, nil
		}
		n.isLeftEmpty = true
	}
	ts, err := n.right.GetTuples(limit)
	if err != nil {
		return nil, err
	}
	if len(ts) == 0 {
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
