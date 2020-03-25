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

func (n *restrict) AttributeList() ([]string, error) {
	return n.prev.AttributeList()
}

func (n *restrict) GetTuples(limit int) (value.Array, error) {
	var a value.Array

	attrs, err := n.prev.AttributeList()
	if err != nil {
		return nil, err
	}
	ts, err := n.prev.GetTuples(limit)
	if err != nil {
		return nil, err
	}
	for i, j := 0, len(ts); i < j; i++ {
		if ok, err := n.e.Eval(util.Tuple2Map(ts[i].(value.Array), attrs)); err != nil {
			return nil, err
		} else if value.MustBeBool(ok) {
			a = append(a, ts[i])
		}
	}
	return a, nil
}

func (n *restrict) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	var as [][]string

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
	if len(mp) == 0 {
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
