package group

import (
	"errors"
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/container/dictVec"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload/avg"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload/count"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload/max"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload/min"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload/sum"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/util/encoding"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(prev op.OP, gs []string, es []*summarize.Extend, c context.Context) *group {
	return &group{isCheck: false, prev: prev, gs: gs, c: c, es: es}
}

func (n *group) Name() (string, error) {
	return n.prev.Name()
}

func (n *group) AttributeList() ([]string, error) {
	return aliasList(n.es, n.gs), nil
}

func (n *group) GetTuples(limit int) (value.Array, error) {
	attrs := attributeList(n.es, n.gs)
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		if err := n.newByAttributes(attrs); err != nil {
			n.dv.Destroy()
			return nil, err
		}
		n.isCheck = true
	}
	size := 0
	var a value.Array
	for {
		if size >= limit {
			break
		}
		if len(n.k) == 0 {
			k, err := n.dv.PopKey()
			if err != nil {
				n.dv.Destroy()
				return nil, err
			}
			if len(k) == 0 {
				n.dv.Destroy()
				return a, nil
			}
			n.k = k
		}
		ts, err := n.dv.Pops(n.k, -1, n.c.MemSize())
		switch {
		case err == dictVec.NotExist || (err == nil && len(ts) == 0):
			var t value.Array
			{
				v, _, err := encoding.DecodeValue([]byte(n.k))
				if err != nil {
					n.dv.Destroy()
					return nil, err
				}
				size += v.(value.Array).Size()
				t = append(t, v.(value.Array)...)
			}
			for _, e := range n.es {
				if v, err := e.Agg.Eval(); err != nil {
					n.dv.Destroy()
					return nil, err
				} else {
					size += v.Size()
					t = append(t, v)
				}
				e.Agg.Reset()
			}
			a = append(a, t)
			n.k = ""
			continue
		case err != nil:
			n.dv.Destroy()
			return nil, err
		}
		mp := util.Tuples2Map(ts, attrs)
		for _, e := range n.es {
			if err := e.Agg.Fill(mp[e.Name]); err != nil {
				n.dv.Destroy()
				return nil, err
			}
		}
	}
	return a, nil
}

func (n *group) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	var as [][]string

	es := subExtend(n.es, attrs)
	as = append(as, attributeList(es, n.gs))
	if !n.isCheck {
		if err := n.check(as[0]); err != nil {
			return nil, err
		}
		if err := util.Contain(attrs, aliasList(n.es, n.gs)); err != nil {
			return nil, err
		}
		if err := n.newByAttributes(as[0]); err != nil {
			n.dv.Destroy()
			return nil, err
		}
		n.isCheck = true
	}
	size := 0
	rq := make(map[string]value.Array)
	for {
		if size >= limit {
			break
		}
		if len(n.k) == 0 {
			k, err := n.dv.PopKey()
			if err != nil {
				n.dv.Destroy()
				return nil, err
			}
			if len(k) == 0 {
				n.dv.Destroy()
				return rq, nil
			}
			n.k = k
		}
		ts, err := n.dv.Pops(n.k, -1, n.c.MemSize())
		switch {
		case err == dictVec.NotExist || (err == nil && len(ts) == 0):
			{
				v, _, err := encoding.DecodeValue([]byte(n.k))
				if err != nil {
					n.dv.Destroy()
					return nil, err
				}
				size += v.(value.Array).Size()
				for i, attr := range n.gs {
					rq[attr] = append(rq[attr], v.(value.Array)[i])
				}
			}
			for _, e := range es {
				if v, err := e.Agg.Eval(); err != nil {
					n.dv.Destroy()
					return nil, err
				} else {
					size += v.Size()
					rq[e.Alias] = append(rq[e.Alias], v)
				}
				e.Agg.Reset()
			}
			n.k = ""
			continue
		case err != nil:
			n.dv.Destroy()
			return nil, err
		}
		mp := util.Tuples2Map(ts, as[0])
		for _, e := range es {
			if err := e.Agg.Fill(mp[e.Name]); err != nil {
				n.dv.Destroy()
				return nil, err
			}
		}
	}
	return rq, nil
}

func (n *group) newByAttributes(attrs []string) error {
	limit := n.c.MemSize()
	dv, err := n.c.NewDictVector()
	if err != nil {
		return err
	}
	n.dv = dv
	for {
		mp, err := n.prev.GetAttributes(attrs, limit)
		if err != nil {
			return err
		}
		if len(mp) == 0 || len(mp[attrs[0]]) == 0 {
			return nil
		}
		for i, j := 0, len(mp[attrs[0]]); i < j; i++ {
			k, err := encoding.EncodeValue(util.Map2Tuple(mp, n.gs, i))
			if err != nil {
				return err
			}
			if err := n.dv.Push(string(k), value.Array{util.Map2Tuple(mp, attrs, i)}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *group) check(attrs []string) error {
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

func aliasList(es []*summarize.Extend, attrs []string) []string {
	var rs []string

	for _, e := range es {
		rs = append(rs, e.Alias)
	}
	return util.MergeAttributes(attrs, rs)
}

func attributeList(es []*summarize.Extend, attrs []string) []string {
	var rs []string

	for _, e := range es {
		rs = append(rs, e.Name)
	}
	return util.MergeAttributes(attrs, rs)
}

func subExtend(es []*summarize.Extend, attrs []string) []*summarize.Extend {
	var rs []*summarize.Extend

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
