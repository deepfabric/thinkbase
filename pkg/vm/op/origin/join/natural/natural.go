package natural

import (
	"errors"
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/util"
	"github.com/deepfabric/thinkbase/pkg/vm/util/encoding"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(left, right op.OP, c context.Context) *join {
	if c.Less(right, left) {
		left, right = right, left
	}
	return &join{isCheck: false, left: left, right: right, c: c}
}

func (n *join) Name() (string, error) {
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

func (n *join) AttributeList() ([]string, error) {
	lattrs, err := n.left.AttributeList()
	if err != nil {
		return nil, err
	}
	rattrs, err := n.right.AttributeList()
	if err != nil {
		return nil, err
	}
	var rs []string
	mp := make(map[string]struct{})
	for _, attr := range rattrs {
		rs = append(rs, attr)
		mp[attr] = struct{}{}
	}
	for _, attr := range lattrs {
		if _, ok := mp[attr]; !ok {
			rs = append(rs, attr)
		}
	}
	return rs, nil
}

func (n *join) GetTuples(limit int) (value.Array, error) {
	if !n.isCheck {
		if err := n.commonAttributeList(); err != nil {
			return nil, err
		}
		lis, err := n.leftIndexs()
		if err != nil {
			return nil, err
		}
		n.lis = lis
		ris, err := n.rightIndexs()
		if err != nil {
			return nil, err
		}
		n.ris = ris
		dv, err := n.c.NewDictVector()
		if err != nil {
			return nil, err
		}
		n.dv = dv
		if err := n.newByTuple(); err != nil {
			n.dv.Destroy()
			return nil, err
		}
		n.isCheck = true
	}
	ts, err := n.right.GetTuples(limit)
	if err != nil {
		n.dv.Destroy()
		return nil, err
	}
	if len(ts) == 0 {
		n.dv.Destroy()
		return nil, nil
	}
	var a value.Array
	for i, j := 0, len(ts); i < j; i++ {
		k, err := encoding.EncodeValue(util.SubTuple(ts[i].(value.Array), n.ris))
		if err != nil {
			n.dv.Destroy()
			return nil, err
		}
		length, err := n.dv.Len(string(k))
		if err != nil {
			n.dv.Destroy()
			return nil, err
		}
		if length == 0 {
			continue
		}
		for idx := 0; idx < length; idx++ {
			t, err := n.dv.Get(string(k), idx)
			if err != nil {
				n.dv.Destroy()
				return nil, err
			}
			a = append(a, append(ts[i].(value.Array), util.SubTuple(t.(value.Array), n.lis)...))
		}
	}
	return a, nil
}

func (n *join) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	if !n.isCheck {
		if err := n.check(attrs); err != nil {
			return nil, err
		}
		if err := n.commonAttributeList(); err != nil {
			return nil, err
		}
		lis, err := n.leftIndexs()
		if err != nil {
			return nil, err
		}
		n.lis = lis
		ris, err := n.rightIndexs()
		if err != nil {
			return nil, err
		}
		n.ris = ris
		dv, err := n.c.NewDictVector()
		if err != nil {
			return nil, err
		}
		n.dv = dv
		if err := n.newByTuple(); err != nil {
			n.dv.Destroy()
			return nil, err
		}
		n.isCheck = true
	}
	is, err := n.indexs(attrs)
	if err != nil {
		n.dv.Destroy()
		return nil, err
	}
	ts, err := n.right.GetTuples(limit)
	if err != nil {
		n.dv.Destroy()
		return nil, err
	}
	if len(ts) == 0 {
		n.dv.Destroy()
		return nil, nil
	}
	rq := make(map[string]value.Array)
	for i, j := 0, len(ts); i < j; i++ {
		k, err := encoding.EncodeValue(util.SubTuple(ts[i].(value.Array), n.ris))
		if err != nil {
			n.dv.Destroy()
			return nil, err
		}
		length, err := n.dv.Len(string(k))
		if err != nil {
			n.dv.Destroy()
			return nil, err
		}
		if length == 0 {
			continue
		}
		for idx := 0; idx < length; idx++ {
			t, err := n.dv.Get(string(k), idx)
			if err != nil {
				n.dv.Destroy()
				return nil, err
			}
			a := append(ts[i].(value.Array), util.SubTuple(t.(value.Array), n.lis)...)
			for idx, attr := range attrs {
				rq[attr] = append(rq[attr], a[is[idx]])
			}
		}
	}
	return rq, nil
}

func (n *join) leftIndexs() ([]int, error) {
	var rs []int

	mp := make(map[string]struct{})
	attrs, err := n.left.AttributeList()
	if err != nil {
		return nil, err
	}
	for _, attr := range n.attrs {
		mp[attr] = struct{}{}
	}
	for i, attr := range attrs {
		if _, ok := mp[attr]; !ok {
			rs = append(rs, i)
		}
	}
	return rs, nil
}

func (n *join) rightIndexs() ([]int, error) {
	attrs, err := n.right.AttributeList()
	if err != nil {
		return nil, err
	}
	return util.Indexs(n.attrs, attrs), nil
}

func (n *join) commonAttributeList() error {
	lattrs, err := n.left.AttributeList()
	if err != nil {
		return err
	}
	rattrs, err := n.right.AttributeList()
	if err != nil {
		return err
	}
	var attrs []string
	mp := make(map[string]struct{})
	for _, attr := range rattrs {
		mp[attr] = struct{}{}
	}
	for _, attr := range lattrs {
		if _, ok := mp[attr]; ok {
			attrs = append(attrs, attr)
		}
	}
	if len(attrs) == 0 {
		return errors.New("no common attributes")
	}
	n.attrs = attrs
	return nil
}

func (n *join) newByTuple() error {
	limit := n.c.MemSize()
	attrs, err := n.left.AttributeList()
	if err != nil {
		return err
	}
	is := util.Indexs(n.attrs, attrs)
	for {
		ts, err := n.left.GetTuples(limit)
		if err != nil {
			return err
		}
		if len(ts) == 0 {
			return nil
		}
		for i, j := 0, len(ts); i < j; i++ {
			k, err := encoding.EncodeValue(util.SubTuple(ts[i].(value.Array), is))
			if err != nil {
				return err
			}
			if err := n.dv.Push(string(k), value.Array{ts[i]}); err != nil {
				return err
			}
		}
	}
}

func (n *join) indexs(attrs []string) ([]int, error) {
	as, err := n.AttributeList()
	if err != nil {
		return nil, err
	}
	return util.Indexs(attrs, as), nil
}

func (n *join) check(attrs []string) error {
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
