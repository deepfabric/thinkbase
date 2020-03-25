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

func (n *projection) AttributeList() ([]string, error) {
	return aliasList(n.es), nil
}

func (n *projection) GetTuples(limit int) (value.Array, error) {
	var a value.Array

	attrs := attributeList(n.es)
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	mp, err := n.prev.GetAttributes(attrs, limit)
	if err != nil {
		return nil, err
	}
	if len(mp) == 0 {
		return nil, nil
	}
	for i, j := 0, len(mp[attrs[0]]); i < j; i++ {
		var t value.Array
		for _, e := range n.es {
			v, err := e.E.Eval(util.SubMap(mp, attrs, i))
			if err != nil {
				return nil, err
			}
			t = append(t, v)
		}
		a = append(a, t)
	}
	return a, nil
}

func (n *projection) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	var as [][]string

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
	if len(mp) == 0 {
		return mp, nil
	}
	rq := make(map[string]value.Array)
	for i, j := 0, len(mp[as[0][0]]); i < j; i++ {
		for _, e := range es {
			v, err := e.E.Eval(util.SubMap(mp, e.E.Attributes(), i))
			if err != nil {
				return nil, err
			}
			switch t := e.E.(type) {
			case *extend.Attribute:
				rq[t.Name] = append(rq[t.Name], v)
			default:
				rq[e.Alias] = append(rq[e.Alias], v)
			}
		}
	}
	return rq, nil
}

func (n *projection) check(attrs []string) error {
	{
		for _, e := range n.es {
			if _, ok := e.E.(*extend.Attribute); !ok {
				if len(e.Alias) == 0 {
					return errors.New("need alias")
				}
			}
			if len(e.E.Attributes()) <= 0 {
				return errors.New("must act on attributes")
			}
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
		switch t := e.E.(type) {
		case *extend.Attribute:
			rs = append(rs, t.Name)
		default:
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
