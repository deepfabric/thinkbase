package projection

import (
	"errors"
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(prev op.OP, es []*Extend, c context.Context) *projection {
	return &projection{false, prev, es, c}
}

func (n *projection) Size() float64 {
	var as []string
	var es []extend.Extend

	for _, e := range n.es {
		es = append(es, e.E)
		as = append(as, e.Alias)
	}
	return n.c.ProjectionSize(n.prev, as, es)
}

func (n *projection) Cost() float64 {
	var as []string
	var es []extend.Extend

	for _, e := range n.es {
		es = append(es, e.E)
		as = append(as, e.Alias)
	}
	return n.c.ProjectionCost(n.prev, as, es)
}

func (n *projection) Dup() op.OP {
	return &projection{
		c:       n.c,
		es:      n.es,
		prev:    n.prev,
		isCheck: n.isCheck,
	}
}

func (n *projection) SetChild(o op.OP, _ int) { n.prev = o }
func (n *projection) Operate() int            { return op.Projection }
func (n *projection) Children() []op.OP       { return []op.OP{n.prev} }
func (n *projection) IsOrdered() bool         { return n.prev.IsOrdered() }

func (n *projection) String() string {
	r := fmt.Sprintf("π([")
	for i, e := range n.es {
		switch i {
		case 0:
			if len(e.Alias) == 0 {
				r += fmt.Sprintf("%s", e.E)
			} else {
				r += fmt.Sprintf("%s -> %s", e.E, e.Alias)
			}
		default:
			if len(e.Alias) == 0 {
				r += fmt.Sprintf(", %s", e.E)
			} else {
				r += fmt.Sprintf(", %s -> %s", e.E, e.Alias)
			}
		}
	}
	r += fmt.Sprintf("], %s)", n.prev)
	return r
}

func (n *projection) Name() (string, error) {
	return n.prev.Name()
}

func (n *projection) AttributeList() ([]string, error) {
	return aliasList(n.es), nil
}

func (n *projection) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	var as [][]string

	attrs = util.MergeAttributes(attrs, []string{})
	as = append(as, attributeList(n.es)) // extend's Attributes
	if !n.isCheck {
		if err := n.check(as[0]); err != nil {
			return nil, err
		}
		if err := util.Contain(attrs, aliasList(n.es)); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	es := subExtend(n.es, attrs)
	as[0] = attributeList(es)
	mp, err := n.prev.GetAttributes(as[0], limit)
	if err != nil {
		return nil, err
	}
	if len(mp) == 0 || len(mp[as[0][0]]) == 0 {
		return mp, nil
	}
	rq := make(map[string]value.Array)
	for i, j := 0, len(mp[as[0][0]]); i < j; i++ {
		for _, e := range es {
			v, err := e.E.Eval(util.SubMap(mp, e.E.Attributes(), i))
			if err != nil {
				return nil, err
			}
			if t, ok := e.E.(*extend.Attribute); ok && len(e.Alias) == 0 {
				rq[t.Name] = append(rq[t.Name], v)
			} else {
				rq[e.Alias] = append(rq[e.Alias], v)
			}
		}
	}
	return rq, nil
}

func (n *projection) check(attrs []string) error {
	for _, e := range n.es {
		if len(e.E.Attributes()) <= 0 {
			return errors.New("must act on attributes")
		}
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

func aliasList(es []*Extend) []string {
	var rs []string

	for _, e := range es {
		if t, ok := e.E.(*extend.Attribute); ok && len(e.Alias) == 0 {
			rs = append(rs, t.Name)
		} else {
			rs = append(rs, e.Alias)
		}
	}
	return rs
}

func attributeList(es []*Extend) []string {
	var rs []string

	mp := make(map[string]struct{})
	for _, e := range es {
		as := e.E.Attributes()
		for i, j := 0, len(as); i < j; i++ {
			if _, ok := mp[as[i]]; !ok {
				mp[as[i]] = struct{}{}
				rs = append(rs, as[i])
			}
		}
	}
	return rs
}

func subExtend(es []*Extend, attrs []string) []*Extend {
	var rs []*Extend

	mp := make(map[string]struct{})
	for i, j := 0, len(attrs); i < j; i++ {
		mp[attrs[i]] = struct{}{}
	}
	as := aliasList(es)
	for i, j := 0, len(es); i < j; i++ {
		if _, ok := mp[as[i]]; ok {
			rs = append(rs, es[i])
		}
	}
	return rs
}
