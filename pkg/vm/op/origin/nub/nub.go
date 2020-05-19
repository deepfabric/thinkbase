package nub

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(prev op.OP, attrs []string, c context.Context) *nub {
	return &nub{isCheck: false, prev: prev, attrs: attrs, c: c}
}

func (n *nub) Size() float64 {
	return n.c.NubSize(n.prev, n.attrs)
}

func (n *nub) Cost() float64 {
	return n.c.NubCost(n.prev, n.attrs)
}

func (n *nub) Dup() op.OP {
	return &nub{
		c:       n.c,
		prev:    n.prev,
		attrs:   n.attrs,
		isCheck: n.isCheck,
	}
}

func (n *nub) SetChild(o op.OP, _ int) { n.prev = o }
func (n *nub) Operate() int            { return op.Nub }
func (n *nub) Children() []op.OP       { return []op.OP{n.prev} }
func (n *nub) IsOrdered() bool         { return n.prev.IsOrdered() }

func (n *nub) String() string {
	r := fmt.Sprintf("δ([")
	for i, attr := range n.attrs {
		switch i {
		case 0:
			r += fmt.Sprintf("%s", attr)
		default:
			r += fmt.Sprintf(", %s", attr)
		}
	}
	r += fmt.Sprintf("], %s)", n.prev)
	return r
}

func (n *nub) Name() (string, error) {
	return n.prev.Name()
}

func (n *nub) AttributeList() ([]string, error) {
	return n.prev.AttributeList()
}

func (n *nub) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	var as [][]string

	attrs = util.MergeAttributes(attrs, []string{})
	as = append(as, util.MergeAttributes(attrs, n.attrs))
	if !n.isCheck {
		if err := n.check(as[0]); err != nil {
			return nil, err
		}
		if dict, err := n.c.NewDictionary(); err != nil {
			return nil, err
		} else {
			n.dict = dict
		}
		n.isCheck = true
	}
	rq := make(map[string]value.Array)
	for {
		mp, err := n.prev.GetAttributes(as[0], limit)
		if err != nil {
			n.dict.Destroy()
			return nil, err
		}
		if len(mp) == 0 || len(mp[attrs[0]]) == 0 {
			n.dict.Destroy()
			return nil, nil
		}
		for i, j := 0, len(mp[attrs[0]]); i < j; i++ {
			if ok, err := n.dict.GetOrSet(util.Map2Tuple(mp, n.attrs, i)); err != nil {
				n.dict.Destroy()
				return nil, err
			} else if !ok {
				for _, attr := range attrs {
					rq[attr] = append(rq[attr], mp[attr][i])
				}
			}
		}
		if len(rq) > 0 {
			break
		}
	}
	return rq, nil
}

func (n *nub) check(attrs []string) error {
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
