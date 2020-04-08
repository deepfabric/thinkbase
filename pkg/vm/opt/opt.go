package opt

import (
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/opt/rule"
	"github.com/deepfabric/thinkbase/pkg/vm/opt/rule/rule0"
	"github.com/deepfabric/thinkbase/pkg/vm/opt/rule/rule1"
	"github.com/deepfabric/thinkbase/pkg/vm/opt/rule/rule2"
)

func New(o op.OP) *optimizer {
	return &optimizer{o}
}

func (o *optimizer) Optimize() op.OP {
	g, _ := o.optimizeGroup(o.o, make(map[string]op.OP))
	return g
}

func (o *optimizer) optimizeGroup(g op.OP, mp map[string]op.OP) (op.OP, bool) {
	for {
		ok := false
		state := true
		children := g.Children()
		for _, child := range children {
			mp[child.String()] = g
			_, ok = o.optimizeGroup(child, mp)
			state = state && !ok
		}
		g, ok = o.explore(g, mp)
		if state && !ok {
			return g, false
		}
	}
	return g, false
}

func (o *optimizer) explore(g op.OP, mp map[string]op.OP) (op.OP, bool) {
	if rs, ok := Rules[g.Operate()]; ok {
		for _, r := range rs {
			if r.Match(g) {
				return r.Rewrite(g, mp)
			}
		}
	}
	return g, false
}

var Rules = map[int][]rule.Rule{
	op.Restrict: []rule.Rule{rule0.New(), rule1.New(), rule2.New()},

	//	op.SetUnion: []rule.Rule{rule100.New()},
}
