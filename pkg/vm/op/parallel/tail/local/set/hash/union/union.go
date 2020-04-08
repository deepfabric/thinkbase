package union

import (
	"errors"
	"fmt"
	"sync"

	"github.com/deepfabric/thinkbase/pkg/vm/container/vector"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(r, s op.OP, c context.Context) *union {
	return &union{
		c:       c,
		r:       r,
		s:       s,
		isCheck: false,
	}
}

func (n *union) Size() float64 {
	return n.c.SetUnionSizeByHash(n.r, n.s)
}

func (n *union) Cost() float64 {
	return n.c.SetUnionCostByHash(n.r, n.s)
}

func (n *union) Dup() op.OP {
	return &union{
		c:       n.c,
		r:       n.r,
		s:       n.s,
		isCheck: n.isCheck,
	}
}

func (n *union) Operate() int {
	return op.SetUnion
}

func (n *union) Children() []op.OP {
	return []op.OP{n.r, n.s}
}

func (n *union) SetChild(o op.OP, idx int) {
	switch idx {
	case 0:
		n.r = o
	default:
		n.s = o
	}
}

func (n *union) IsOrdered() bool {
	return false
}

func (n *union) String() string {
	return fmt.Sprintf("(%s ∪  %s, hash union)", n.r, n.s)
}

func (n *union) Name() (string, error) {
	rn, err := n.r.Name()
	if err != nil {
		return "", err
	}
	sn, err := n.s.Name()
	if err != nil {
		return "", err
	}
	return rn + "." + sn, nil
}

func (n *union) AttributeList() ([]string, error) {
	return n.r.AttributeList()
}

func (n *union) GetTuples(limit int) (value.Array, error) {
	if !n.isCheck {
		if err := n.check(nil); err != nil {
			return nil, err
		}
		if err := n.newByTuple(limit); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	if len(n.vs) == 0 {
		return nil, nil
	}
	for {
		a, err := n.vs[0].Pops(-1, limit)
		if err != nil {
			for _, v := range n.vs {
				v.Destroy()
			}
			return nil, err
		}
		if len(a) == 0 {
			n.vs[0].Destroy()
			n.vs[0] = nil
			if n.vs = n.vs[1:]; len(n.vs) == 0 {
				return nil, nil
			}
		}
		return a, nil
	}
}

func (n *union) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	as, err := n.AttributeList()
	if err != nil {
		return nil, err
	}
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		if err := n.newByTuple(limit); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	if len(n.vs) == 0 {
		return nil, nil
	}
	rq := make(map[string]value.Array)
	for {
		a, err := n.vs[0].Pops(-1, limit)
		if err != nil {
			for _, v := range n.vs {
				v.Destroy()
			}
			return nil, err
		}
		if len(a) == 0 {
			n.vs[0].Destroy()
			n.vs[0] = nil
			if n.vs = n.vs[1:]; len(n.vs) == 0 {
				return nil, nil
			}
		}
		mp := util.Tuples2Map(a, as)
		for _, attr := range attrs {
			rq[attr] = append(rq[attr], mp[attr]...)
		}
		return rq, nil
	}
}

func (n *union) newByTuple(limit int) error {
	mcpu := n.c.NumMcpu()
	rh, err := n.c.NewHash(mcpu)
	if err != nil {
		return err
	}
	for {
		ts, err := n.r.GetTuples(limit)
		if err != nil {
			rh.Destroy()
			return err
		}
		if len(ts) == 0 {
			break
		}
		for _, t := range ts {
			if err := rh.Set(t); err != nil {
				rh.Destroy()
				return err
			}
		}
	}
	sh, err := n.c.NewHash(mcpu)
	if err != nil {
		rh.Destroy()
		return err
	}
	for {
		ts, err := n.s.GetTuples(limit)
		if err != nil {
			rh.Destroy()
			sh.Destroy()
			return err
		}
		if len(ts) == 0 {
			break
		}
		for _, t := range ts {
			if err := sh.Set(t); err != nil {
				rh.Destroy()
				sh.Destroy()
				return err
			}
		}
	}
	defer func() {
		rh.Destroy()
		sh.Destroy()
	}()
	var wg sync.WaitGroup
	n.vs = make([]vector.Vector, mcpu)
	if limit = limit / mcpu; limit < 1024 {
		limit = 1024
	}
	for i := 0; i < mcpu; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			a, privErr := rh.Pop(idx)
			if privErr != nil {
				err = privErr
				return
			}
			b, privErr := sh.Pop(idx)
			if privErr != nil {
				err = privErr
				return
			}
			v, privErr := vectorUnion(limit, a, b, n.c)
			if privErr != nil {
				err = privErr
				return
			}
			n.vs[idx] = v
		}(i)
	}
	wg.Wait()
	if err != nil {
		for i := 0; i < mcpu; i++ {
			if n.vs[i] != nil {
				n.vs[i].Destroy()
			}
		}
		return err
	}
	return nil
}

func (n *union) check(attrs []string) error {
	{
		rattrs, err := n.r.AttributeList()
		if err != nil {
			return err
		}
		sattrs, err := n.s.AttributeList()
		if err != nil {
			return err
		}
		if len(rattrs) != len(sattrs) {
			return errors.New("attribute not equal")
		}
		for i, j := 0, len(rattrs); i < j; i++ {
			if rattrs[i] != sattrs[i] {
				return errors.New("attribute not equal")
			}
		}
	}
	if len(attrs) == 0 {
		return nil
	}
	as, err := n.r.AttributeList()
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

func vectorUnion(limit int, a, b vector.Vector, c context.Context) (vector.Vector, error) {
	v, err := c.NewVector()
	if err != nil {
		return nil, err
	}
	dict, err := c.NewDictionary()
	if err != nil {
		v.Destroy()
		return nil, err
	}
	defer dict.Destroy()
	for {
		ts, err := a.Pops(-1, limit)
		if err != nil {
			v.Destroy()
			dict.Destroy()
			return nil, err
		}
		if len(ts) == 0 {
			break
		}
		for i, j := 0, len(ts); i < j; i++ {
			if ok, _, err := dict.GetOrSet(ts[i], nil); err != nil {
				v.Destroy()
				dict.Destroy()
				return nil, err
			} else if !ok {
				v.Append(value.Array{ts[i]})
			}
		}
	}
	for {
		ts, err := b.Pops(-1, limit)
		if err != nil {
			v.Destroy()
			dict.Destroy()
			return nil, err
		}
		if len(ts) == 0 {
			break
		}
		for i, j := 0, len(ts); i < j; i++ {
			if ok, _, err := dict.GetOrSet(ts[i], nil); err != nil {
				v.Destroy()
				dict.Destroy()
				return nil, err
			} else if !ok {
				v.Append(value.Array{ts[i]})
			}
		}
	}
	return v, nil
}
