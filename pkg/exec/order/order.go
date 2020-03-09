package order

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func New(us []unit.Unit, c context.Context, cmp func(value.Tuple, value.Tuple) bool) *order {
	return &order{us, c, cmp}
}

func (e *order) Order() (relation.Relation, error) {
	var err error
	var wg sync.WaitGroup

	rs := make([]relation.Relation, len(e.us))
	for i, j := 0, len(e.us); i < j; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r, privErr := e.us[idx].Result()
			if err != nil {
				err = privErr
			}
			rs[idx] = r
		}(i)
	}
	wg.Wait()
	if err != nil {
		return nil, err
	}
	ts, err := e.summary(rs)
	if err != nil {
		return nil, err
	}
	r := mem.New("", rs[0].Metadata(), e.c)
	r.AddTuples(ts)
	return r, nil
}

func (e *order) summary(rs []relation.Relation) ([]value.Tuple, error) {
	switch len(rs) {
	case 0:
		return nil, nil
	case 1:
		return util.GetTuples(rs[0])
	case 2:
		lts, err := util.GetTuples(rs[0])
		if err != nil {
			return nil, err
		}
		rts, err := util.GetTuples(rs[1])
		if err != nil {
			return nil, err
		}
		return e.merge(lts, rts), nil
	default:
		var lerr, rerr error
		var wg sync.WaitGroup
		var lts, rts []value.Tuple

		wg.Add(2)
		go func() { lts, lerr = e.summary(rs[:len(rs)/2]); wg.Done() }()
		go func() { rts, rerr = e.summary(rs[len(rs)/2:]); wg.Done() }()
		wg.Wait()
		if lerr != nil {
			return nil, lerr
		}
		if rerr != nil {
			return nil, rerr
		}
		return e.merge(lts, rts), nil
	}
}

func (e *order) merge(xs, ys []value.Tuple) []value.Tuple {
	var rs []value.Tuple

	for len(xs) > 0 && len(ys) > 0 {
		if e.cmp(xs[0], ys[0]) {
			rs = append(rs, xs[0])
			xs = xs[1:]
		} else {
			rs = append(rs, ys[0])
			ys = ys[1:]
		}
	}
	if len(xs) > 0 {
		rs = append(rs, xs...)
	}
	if len(ys) > 0 {
		rs = append(rs, ys...)
	}
	return rs
}
