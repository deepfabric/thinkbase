package restrict

import (
	"errors"
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(prev op.OP, e extend.Extend, c context.Context) *restrict {
	return &restrict{false, prev, e, c}
}

func (n *restrict) Extend() extend.Extend {
	return n.e
}

func (n *restrict) Size() float64 {
	return n.c.RestrictSize(n.prev, n.e)
}

func (n *restrict) Cost() float64 {
	return n.c.RestrictCost(n.prev, n.e)
}

func (n *restrict) Dup() op.OP {
	return &restrict{
		e:       n.e,
		c:       n.c,
		prev:    n.prev,
		isCheck: n.isCheck,
	}
}

func (n *restrict) SetChild(o op.OP, _ int) { n.prev = o }
func (n *restrict) Operate() int            { return op.Restrict }
func (n *restrict) Children() []op.OP       { return []op.OP{n.prev} }
func (n *restrict) IsOrdered() bool         { return n.prev.IsOrdered() }

func (n *restrict) String() string {
	return fmt.Sprintf("σ(%s, %s)", n.e, n.prev)
}

func (n *restrict) Name() (string, error) {
	return n.prev.Name()
}

func (n *restrict) AttributeList() ([]string, error) {
	return n.prev.AttributeList()
}

func (n *restrict) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	var as [][]string

	attrs = util.MergeAttributes(attrs, []string{})
	as = append(as, n.e.Attributes()) // extend's Attributes
	as = append(as, util.MergeAttributes(attrs, as[0]))
	if !n.isCheck {
		if err := n.check(as[1]); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	mp, err := n.prev.GetAttributes(as[1], limit)
	if err != nil {
		return nil, err
	}
	if len(mp) == 0 || len(mp[attrs[0]]) == 0 {
		return mp, nil
	}
	rq := make(map[string]value.Array)
	for i, j := 0, len(mp[attrs[0]]); i < j; i++ {
		if ok, err := n.e.Eval(util.SubMap(mp, as[0], i)); err != nil {
			return nil, err
		} else if value.MustBeBool(ok) {
			for _, attr := range attrs {
				rq[attr] = append(rq[attr], mp[attr][i])
			}
		}
	}
	return rq, nil
}

func (n *restrict) check(attrs []string) error {
	if !n.e.IsLogical() {
		return errors.New("extend must be a boolean expression")
	}
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
