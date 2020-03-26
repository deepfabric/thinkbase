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

func (n *nub) AttributeList() ([]string, error) {
	return n.prev.AttributeList()
}

func (n *nub) GetTuples(limit int) (value.Array, error) {
	var a value.Array

	if !n.isCheck {
		if err := n.check(n.attrs); err != nil {
			return nil, err
		}
		if dict, err := n.c.NewDictionary(); err != nil {
			return nil, err
		} else {
			n.dict = dict
		}
		n.isCheck = true
	}
	attrs, err := n.prev.AttributeList()
	if err != nil {
		n.dict.Destroy()
		return nil, err
	}
	ts, err := n.prev.GetTuples(limit)
	if err != nil {
		n.dict.Destroy()
		return nil, err
	}
	if len(ts) == 0 {
		n.dict.Destroy()
		return ts, nil
	}
	is := util.Indexs(n.attrs, attrs)
	for i, j := 0, len(ts); i < j; i++ {
		if ok, _, err := n.dict.GetOrSet(util.SubTuple(ts[i].(value.Array), is), nil); err != nil {
			n.dict.Destroy()
			return nil, err
		} else if !ok {
			a = append(a, ts[i])
		}
	}
	return a, nil
}

func (n *nub) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	var as [][]string

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
	mp, err := n.prev.GetAttributes(as[0], limit)
	if err != nil {
		n.dict.Destroy()
		return nil, err
	}
	if len(mp) == 0 || len(mp[attrs[0]]) == 0 {
		n.dict.Destroy()
		return mp, nil
	}
	rq := make(map[string]value.Array)
	for i, j := 0, len(mp[attrs[0]]); i < j; i++ {
		if ok, _, err := n.dict.GetOrSet(util.Map2Tuple(mp, attrs, i), nil); err != nil {
			n.dict.Destroy()
			return nil, err
		} else if !ok {
			for _, attr := range attrs {
				rq[attr] = append(rq[attr], mp[attr][i])
			}
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
