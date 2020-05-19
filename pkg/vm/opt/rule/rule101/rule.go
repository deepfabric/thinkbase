package rule101

import (
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/index/restrict"
	isummarize "github.com/deepfabric/thinkbase/pkg/vm/op/index/summarize"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload"
	Rule "github.com/deepfabric/thinkbase/pkg/vm/opt/rule"
	"github.com/deepfabric/thinkbase/pkg/vm/types"
)

func New(c context.Context) Rule.Rule {
	return &rule{c}
}

func (r *rule) Match(o op.OP, _ map[string]op.OP) bool {
	if o.Operate() != op.Summarize {
		return false
	}
	_, ok := o.Children()[0].(restrict.RestrictOP)
	return ok
}

func (r *rule) Rewrite(o op.OP, mp map[string]op.OP, _, _ map[string]int32) (op.OP, bool) {
	var nes []*isummarize.Extend

	fl := o.Children()[0].(restrict.RestrictOP).Filter()
	prev := o.Children()[0].Children()[0].(relation.Relation)
	es := o.(summarize.SummarizeOP).Extends()
	for _, e := range es {
		switch {
		case overload.IsIndexAggFunc(e.Op):
		case overload.IsIndexTryAggFunc(e.Op):
			if !r.summarizeCost(int32(e.Typ), prev, e) {
				return o, false
			}
		default:
			return o, false
		}
		nes = append(nes, &isummarize.Extend{
			Typ:   e.Typ,
			Name:  e.Name,
			Alias: e.Alias,
			Op:    overload.Convert(e.Op),
		})
	}
	no := isummarize.New(prev, fl, nes, r.c)
	if parent, ok := mp[o.String()]; ok {
		ps := parent.String()
		children := parent.Children()
		for i, child := range children {
			if child == o {
				parent.SetChild(no, i)
				break
			}
		}
		mp[no.String()] = parent
		if gparent, ok := mp[ps]; ok {
			mp[parent.String()] = gparent
		}
	} else {
		mp[""] = no
	}
	return no, true
}

func (r *rule) summarizeCost(typ int32, _ relation.Relation, e *summarize.Extend) bool {
	if typ == types.T_string && overload.IsMax(e.Op) {
		return false
	}
	return true
}
