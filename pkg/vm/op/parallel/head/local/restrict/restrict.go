package restrict

import (
	"fmt"
	"sync"

	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	orestrict "github.com/deepfabric/thinkbase/pkg/vm/op/origin/restrict"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(e extend.Extend, c context.Context, r relation.Relation) *restrict {
	var ops []op.OP

	rs, err := r.Split(c.NumRcpu())
	if err != nil {
		return nil
	}
	for i, j := 0, len(rs); i < j; i++ {
		ops = append(ops, orestrict.New(rs[i], e, c))
	}
	return &restrict{c: c, isCheck: false, ops: ops}
}

func (n *restrict) Name() (string, error) {
	return n.ops[0].Name()
}

func (n *restrict) AttributeList() ([]string, error) {
	return n.ops[0].AttributeList()
}

func (n *restrict) GetTuples(limit int) (value.Array, error) {
	if !n.isCheck {
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
		} else {
			return a, nil
		}
	}
}

func (n *restrict) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		if err := n.newByAttributes(attrs, limit); err != nil {
			return nil, err
		}
		n.isCheck = true
	}
	if len(n.dvs) == 0 {
		return nil, nil
	}
	for {
		mp, err := n.dvs[0].PopsAll(-1, limit)
		if err != nil {
			for _, dv := range n.dvs {
				dv.Destroy()
			}
			return nil, err
		}
		if len(mp) == 0 {
			n.dvs[0].Destroy()
			n.dvs[0] = nil
			if n.dvs = n.dvs[1:]; len(n.dvs) == 0 {
				return nil, nil
			}
		} else {
			return mp, err
		}
	}
}

func (n *restrict) newByTuple(limit int) error {
	var err error
	var wg sync.WaitGroup

	for i, j := 0, len(n.ops); i < j; i++ {
		v, err := n.c.NewVector()
		if err != nil {
			for _, v := range n.vs {
				v.Destroy()
			}
			return err
		}
		n.vs = append(n.vs, v)
	}
	if limit = limit / len(n.ops); limit < 1024 {
		limit = 1024
	}
	for i, j := 0, len(n.ops); i < j; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ts, privErr := n.ops[idx].GetTuples(limit)
			if privErr != nil {
				err = privErr
				return
			}
			if privErr := n.vs[idx].Append(ts); privErr != nil {
				err = privErr
				return
			}
		}(i)
	}
	wg.Wait()
	if err != nil {
		for _, v := range n.vs {
			v.Destroy()
		}
	}
	return err
}

func (n *restrict) newByAttributes(attrs []string, limit int) error {
	var err error
	var wg sync.WaitGroup

	for i, j := 0, len(n.ops); i < j; i++ {
		dv, err := n.c.NewDictVector()
		if err != nil {
			for _, dv := range n.dvs {
				dv.Destroy()
			}
			return err
		}
		n.dvs = append(n.dvs, dv)
	}
	if limit = limit / len(n.ops); limit < 1024 {
		limit = 1024
	}
	for i, j := 0, len(n.ops); i < j; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			mp, privErr := n.ops[idx].GetAttributes(attrs, limit)
			if privErr != nil {
				err = privErr
				return
			}
			for k, v := range mp {
				if privErr := n.dvs[idx].Push(k, v); privErr != nil {
					err = privErr
					return
				}
			}
		}(i)
	}
	wg.Wait()
	if err != nil {
		for _, dv := range n.dvs {
			dv.Destroy()
		}
	}
	return err
}

func (n *restrict) check(attrs []string) error {
	if len(attrs) == 0 {
		return nil
	}
	as, err := n.AttributeList()
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
