package summarize

import (
	"errors"
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload/avg"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload/count"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload/max"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload/min"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload/sum"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(prev op.OP, es []*Extend, c context.Context) *summarize {
	return &summarize{false, false, prev, es, c}
}

func (n *summarize) AttributeList() ([]string, error) {
	return aliasList(n.es), nil
}

func (n *summarize) GetTuples(limit int) (value.Array, error) {
	if n.isUsed {
		return nil, nil
	}
	defer func() { n.isUsed = true }()
	attrs := attributeList(n.es)
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		if err := n.newByAttributes(attrs); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	var a value.Array
	for _, e := range n.es {
		if v, err := e.Agg.Eval(); err != nil {
			return nil, err
		} else {
			a = append(a, v)
		}
	}
	return a, nil
}

func (n *summarize) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	var as [][]string

	if n.isUsed {
		return nil, nil
	}
	defer func() { n.isUsed = true }()
	es := subExtend(n.es, attrs)
	as = append(as, attributeList(es))
	if !n.isCheck {
		if err := n.check(as[0]); err != nil {
			return nil, err
		}
		if err := util.Contain(attrs, aliasList(n.es)); err != nil {
			return nil, err
		}
		if err := n.newByAttributes(as[0]); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	rq := make(map[string]value.Array)
	for _, e := range es {
		if v, err := e.Agg.Eval(); err != nil {
			return nil, err
		} else {
			rq[e.Alias] = append(rq[e.Alias], v)
		}
	}
	return rq, nil
}

func (n *summarize) newByAttributes(attrs []string) error {
	limit := n.c.MemSize()
	for {
		mp, err := n.prev.GetAttributes(attrs, limit)
		if err != nil {
			return err
		}
		if len(mp[attrs[0]]) == 0 {
			return nil
		}
		for _, e := range n.es {
			if err := e.Agg.Fill(mp[e.Name]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *summarize) check(attrs []string) error {
	{
		for i, j := 0, len(n.es); i < j; i++ {
			if len(n.es[i].Name) == 0 {
				return errors.New("need attribute")
			}
			if len(n.es[i].Alias) == 0 {
				return errors.New("need alias")
			}
			switch n.es[i].Op {
			case overload.Avg:
				n.es[i].Agg = avg.New()
			case overload.Max:
				n.es[i].Agg = max.New()
			case overload.Min:
				n.es[i].Agg = min.New()
			case overload.Sum:
				n.es[i].Agg = sum.New()
			case overload.Count:
				n.es[i].Agg = count.New()
			default:
				return fmt.Errorf("unsupport aggreation operator '%v'", n.es[i].Op)
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
		rs = append(rs, e.Alias)
	}
	return rs
}

func attributeList(es []*Extend) []string {
	var rs []string

	for _, e := range es {
		rs = append(rs, e.Name)
	}
	return rs
}

func subExtend(es []*Extend, attrs []string) []*Extend {
	var rs []*Extend

	mp := make(map[string]struct{})
	for i, j := 0, len(attrs); i < j; i++ {
		mp[attrs[i]] = struct{}{}
	}
	for i, j := 0, len(es); i < j; i++ {
		if _, ok := mp[es[i].Alias]; ok {
			rs = append(rs, es[i])
		}
	}
	return rs
}
