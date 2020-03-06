package summarize

import (
	"errors"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/algebra/summarize/overload/avg"
	"github.com/deepfabric/thinkbase/pkg/algebra/summarize/overload/count"
	"github.com/deepfabric/thinkbase/pkg/algebra/summarize/overload/max"
	"github.com/deepfabric/thinkbase/pkg/algebra/summarize/overload/min"
	"github.com/deepfabric/thinkbase/pkg/algebra/summarize/overload/sum"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
)

func New(ops []int, gs []string, as []*Attribute, r relation.Relation) *summarize {
	var is []int
	var aggs []overload.Aggregation

	for _, a := range gs {
		idx, err := r.GetAttributeIndex(a)
		if err != nil {
			return nil
		}
		is = append(is, idx)
	}
	for _, op := range ops {
		if agg, ok := Aggs[op]; !ok {
			return nil
		} else {
			aggs = append(aggs, agg)
		}
	}
	return &summarize{r: r, is: is, gs: gs, as: as, aggs: aggs}
}

func (s *summarize) Summarize(n int) (relation.Relation, error) {
	if len(s.is) > 0 {
		return s.summarizeByGroup(n)
	}
	return s.summarize(n)
}

func (s *summarize) summarize(_ int) (relation.Relation, error) {
	r, err := s.newRelation()
	if err != nil {
		return nil, err
	}
	var t value.Tuple
	for i, attr := range s.as {
		s.aggs[i].Reset()
		a, err := s.r.GetAttribute(attr.Name)
		if err != nil {
			return nil, err
		}
		if err := s.aggs[i].Fill(a); err != nil {
			return nil, err
		}
		if v, err := s.aggs[i].Eval(); err != nil {
			return nil, err
		} else {
			t = append(t, v)
		}
	}
	r.AddTuple(t)
	return r, nil
}

func (s *summarize) summarizeByGroup(n int) (relation.Relation, error) {
	var r relation.Relation

	mp := make(map[string]int)
	switch {
	case len(s.as) > 0:
		var err error

		for _, a := range s.as {
			idx, err := s.r.GetAttributeIndex(a.Name)
			if err != nil {
				return nil, err
			}
			mp[a.Name] = idx
		}
		r, err = s.newRelationByGroup(n)
		if err != nil {
			return nil, err
		}
	default:
		r = mem.New("", s.r.Metadata())
	}
	gs, err := s.group()
	if err != nil {
		return nil, err
	}
	if len(s.as) > 0 {
		for _, g := range gs {
			var t value.Tuple
			for i, attr := range s.as {
				s.aggs[i].Reset()
				if err := s.aggs[i].Fill(g.as[mp[attr.Name]]); err != nil {
					return nil, err
				}
				if v, err := s.aggs[i].Eval(); err != nil {
					return nil, err
				} else {
					t = append(t, v)
				}
			}
			g.r = append(g.r[:n], t...)
		}
	}
	for _, g := range gs {
		r.AddTuple(g.r)
	}
	return r, nil
}

func (s *summarize) newRelation() (relation.Relation, error) {
	var attrs []string

	for _, a := range s.as {
		attr, err := getAttributeName(a)
		if err != nil {
			return nil, err
		}
		attrs = append(attrs, attr)
	}
	return mem.New("", attrs), nil
}

func (s *summarize) newRelationByGroup(n int) (relation.Relation, error) {
	attrs := s.r.Metadata()[:n]
	for _, a := range s.as {
		attr, err := getAttributeName(a)
		if err != nil {
			return nil, err
		}
		attrs = append(attrs, attr)
	}
	return mem.New("", attrs), nil
}

type group struct {
	r  value.Tuple
	as []value.Attribute
}

func (s *summarize) group() ([]*group, error) {
	ts, err := util.GetTuples(s.r)
	if err != nil {
		return nil, err
	}
	gs := []*group{}
	mp := make(map[string]*group)
	for i, j := 0, len(ts); i < j; i++ {
		t := getTuple(ts[i], s.is)
		k := t.String()
		if _, ok := mp[k]; !ok {
			g := &group{r: ts[i]}
			for _, v := range ts[i] {
				g.as = append(g.as, value.Attribute{v})
			}
			mp[k] = g
			gs = append(gs, g)
		} else {
			g := mp[k]
			for i, v := range ts[i] {
				g.as[i] = append(g.as[i], v)
			}
		}
	}
	return gs, nil
}

func getTuple(t value.Tuple, is []int) value.Tuple {
	var r value.Tuple

	for _, i := range is {
		r = append(r, t[i])
	}
	return r
}

func getAttributeName(a *Attribute) (string, error) {
	if len(a.Alias) == 0 {
		return "", errors.New("need alias")
	}
	return a.Alias, nil
}

var Aggs = map[int]overload.Aggregation{
	overload.Avg:   avg.New(),
	overload.Max:   max.New(),
	overload.Min:   min.New(),
	overload.Sum:   sum.New(),
	overload.Count: count.New(),
}
