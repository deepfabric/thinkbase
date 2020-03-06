package testunit

import (
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/summarize"
	"github.com/deepfabric/thinkbase/pkg/algebra/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func NewSummarize(n int, ops []int, gs []string, attrs []*summarize.Attribute, r relation.Relation) ([]unit.Unit, error) {
	{
		var tops []int
		var tattrs []*summarize.Attribute

		flg := false
		for i, j := 0, len(ops); i < j; i++ {
			if ops[i] == overload.Avg {
				flg = true
				tops = append(tops, overload.Sum)
			} else {
				tops = append(tops, ops[i])
			}
			tattrs = append(tattrs, &summarize.Attribute{Name: attrs[i].Name, Alias: attrs[i].Alias})
		}
		ops = tops
		attrs = tattrs
		if flg { // need row count
			ops = append(ops, overload.Count)
			attrs = append(attrs, &summarize.Attribute{Name: r.Metadata()[0], Alias: "_"})
		}
	}
	rs, err := r.Split(n)
	if err != nil {
		return nil, err
	}
	var us []unit.Unit
	for i, j := 0, len(rs); i < j; i++ {
		us = append(us, &summarizeUnit{ops, gs, rs[i], attrs})
	}
	return us, nil
}

func (u *summarizeUnit) Result() (relation.Relation, error) {
	return summarize.New(u.ops, u.gs, u.attrs, u.r).Summarize(len(u.r.Metadata()))
}
