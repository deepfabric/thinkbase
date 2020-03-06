package summarize

import (
	"sync"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	asummarize "github.com/deepfabric/thinkbase/pkg/algebra/summarize"
	"github.com/deepfabric/thinkbase/pkg/algebra/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func New(ops []int, gs []string, attrs []*asummarize.Attribute, r relation.Relation, us []unit.Unit) *summarize {
	var avgs []string

	{
		for i, j := 0, len(ops); i < j; i++ {
			switch ops[i] {
			case overload.Count:
				ops[i] = overload.Sum
			case overload.Avg:
				ops[i] = overload.Sum
				avgs = append(avgs, attrs[i].Alias)
			}
			attrs[i].Name = attrs[i].Alias
		}
		if len(avgs) > 0 { // need row count
			ops = append(ops, overload.Sum)
			attrs = append(attrs, &asummarize.Attribute{Name: "_", Alias: "_"})
		}
	}
	return &summarize{ops, gs, avgs, us, r, attrs}
}

func (e *summarize) Summarize() (relation.Relation, error) {
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
	if sr, err := e.summary(rs); err != nil {
		return nil, err
	} else {
		return e.avg(sr)
	}
}

func (e *summarize) avg(sr relation.Relation) (relation.Relation, error) {
	if len(e.avgs) == 0 {
		return sr, nil
	}
	is, err := e.avgIndexs(e.avgs, sr)
	if err != nil {
		return nil, err
	}
	ts, err := util.GetTuples(sr)
	if err != nil {
		return nil, err
	}
	attrs := sr.Metadata()
	last := len(attrs) - 1
	r := mem.New("", attrs[:last])
	for i, j := 0, len(ts); i < j; i++ {
		for _, idx := range is {
			v := ts[i][idx]
			if _, ok := value.AsInt(v); ok {
				ts[i][idx] = value.NewFloat(float64(value.MustBeInt(v)) / float64(value.MustBeInt(ts[i][last])))
			} else {
				ts[i][idx] = value.NewFloat(value.MustBeFloat(v) / float64(value.MustBeInt(ts[i][last])))
			}
		}
		ts[i] = ts[i][:last]
	}
	r.AddTuples(ts)
	return r, nil
}

func (e *summarize) avgIndexs(as []string, r relation.Relation) ([]int, error) {
	var is []int

	for _, a := range as {
		idx, err := r.GetAttributeIndex(a)
		if err != nil {
			return nil, err
		}
		is = append(is, idx)
	}
	return is, nil
}

func (e *summarize) summary(rs []relation.Relation) (relation.Relation, error) {
	switch len(rs) {
	case 0:
		return nil, nil
	case 1:
		return rs[0], nil
	case 2:
		return e.merge(rs[0], rs[1])
	default:
		var lerr, rerr error
		var wg sync.WaitGroup
		var lr, rr relation.Relation

		wg.Add(2)
		go func() { lr, lerr = e.summary(rs[:len(rs)/2]); wg.Done() }()
		go func() { rr, rerr = e.summary(rs[len(rs)/2:]); wg.Done() }()
		wg.Wait()
		if lerr != nil {
			return nil, lerr
		}
		if rerr != nil {
			return nil, rerr
		}
		return e.merge(lr, rr)
	}
}

func (e *summarize) merge(lr, rr relation.Relation) (relation.Relation, error) {
	ts, err := util.GetTuples(rr)
	if err != nil {
		return nil, err
	}
	lr.AddTuples(ts)
	return asummarize.New(e.ops, e.gs, e.attrs, lr).Summarize(len(e.r.Metadata()))
}
