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
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func New(ops []int, gs []string, as []*Attribute, c context.Context, r relation.Relation) *summarize {
	var aggs []overload.Aggregation

	for _, op := range ops {
		switch op {
		case overload.Avg:
			aggs = append(aggs, avg.New())
		case overload.Max:
			aggs = append(aggs, max.New())
		case overload.Min:
			aggs = append(aggs, min.New())
		case overload.Sum:
			aggs = append(aggs, sum.New())
		case overload.Count:
			aggs = append(aggs, count.New())
		default:
			return nil
		}
	}
	return &summarize{r: r, c: c, gs: gs, as: as, aggs: aggs}
}

func (s *summarize) Summarize() (relation.Relation, error) {
	if len(s.gs) > 0 {
		return s.summarizeByGroup()
	}
	return s.summarize()
}

func (s *summarize) summarize() (relation.Relation, error) {
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

func (s *summarize) summarizeByGroup() (relation.Relation, error) {
	var r relation.Relation

	mp := make(map[string]int)
	switch {
	case len(s.as) > 0:
		var err error

		for i, a := range s.as {
			mp[a.Name] = i
		}
		r, err = s.newRelationByGroup()
		if err != nil {
			return nil, err
		}
	default:
		r = mem.New("", s.r.Metadata(), s.c)
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
			g.r = append(g.r[:len(s.gs)], t...)
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
	return mem.New("", attrs, s.c), nil
}

func (s *summarize) newRelationByGroup() (relation.Relation, error) {
	attrs := s.gs
	for _, a := range s.as {
		attr, err := getAttributeName(a)
		if err != nil {
			return nil, err
		}
		attrs = append(attrs, attr)
	}
	return mem.New("", attrs, s.c), nil
}

type group struct {
	r  value.Tuple
	as []value.Attribute
}

func (s *summarize) group() ([]*group, error) {
	cnt, err := s.r.GetTupleCount()
	if err != nil {
		return nil, err
	}
	xs, ys, err := s.getAttributes()
	if err != nil {
		return nil, err
	}
	gs := []*group{}
	mp := make(map[string]*group)
	for i := 0; i < cnt; i++ {
		kt := getTuple(i, xs)
		k := kt.String()
		if _, ok := mp[k]; !ok {
			g := &group{r: kt}
			t := getTuple(i, ys)
			for _, v := range t {
				g.as = append(g.as, value.Attribute{v})
			}
			mp[k] = g
			gs = append(gs, g)
		} else {
			g := mp[k]
			t := getTuple(i, ys)
			for i, v := range t {
				g.as[i] = append(g.as[i], v)
			}
		}
	}
	return gs, nil
}

func (s *summarize) getAttributes() ([]value.Attribute, []value.Attribute, error) {
	var xs, ys []value.Attribute

	mp := make(map[string]value.Attribute)
	for _, g := range s.gs {
		if x, ok := mp[g]; ok {
			xs = append(xs, x)
		} else {
			x, err := s.r.GetAttribute(g)
			if err != nil {
				return nil, nil, err
			}
			mp[g] = x
			xs = append(xs, x)
		}
	}
	for _, a := range s.as {
		if y, ok := mp[a.Name]; ok {
			ys = append(ys, y)
		} else {
			y, err := s.r.GetAttribute(a.Name)
			if err != nil {
				return nil, nil, err
			}
			mp[a.Name] = y
			ys = append(ys, y)
		}
	}
	return xs, ys, nil
}

func getTuple(i int, as []value.Attribute) value.Tuple {
	var r value.Tuple

	for _, a := range as {
		r = append(r, a[i])
	}
	return r
}

func getAttributeName(a *Attribute) (string, error) {
	if len(a.Alias) == 0 {
		return "", errors.New("need alias")
	}
	return a.Alias, nil
}
