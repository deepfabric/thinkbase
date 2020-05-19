package opt

import (
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/opt/rule"
	"github.com/deepfabric/thinkbase/pkg/vm/opt/rule/rule0"
	"github.com/deepfabric/thinkbase/pkg/vm/opt/rule/rule100"
	"github.com/deepfabric/thinkbase/pkg/vm/opt/rule/rule101"
	"github.com/deepfabric/thinkbase/pkg/vm/opt/rule/rule200"
	"github.com/deepfabric/thinkbase/pkg/vm/opt/rule/rule201"
)

func New(o op.OP, c context.Context) *optimizer {
	rp := make(map[int][]rule.Rule)
	for k, v := range Rules {
		for _, f := range v {
			rp[k] = append(rp[k], f(c))
		}
	}
	return &optimizer{o, rp}
}

func (o *optimizer) Optimize() op.OP {
	mp := make(map[string]op.OP)
	mp[""] = o.o
	o.optimizeGroup(o.o, mp, make(map[string]int32), make(map[string]int32))
	return mp[""]
}

func (o *optimizer) optimizeGroup(g op.OP, mp map[string]op.OP, gmp, gmq map[string]int32) (op.OP, bool) {
	for {
		ok := false
		state := true
		children := g.Children()
		for _, child := range children {
			mp[child.String()] = g
			_, ok = o.optimizeGroup(child, mp, gmp, gmq)
			state = state && !ok
		}
		g, ok = o.explore(g, mp, gmp, gmq)
		if state && !ok {
			return g, false
		}
	}
	return g, false
}

func (o *optimizer) explore(g op.OP, mp map[string]op.OP, gmp, gmq map[string]int32) (op.OP, bool) {
	if rs, ok := o.rp[g.Operate()]; ok {
		for _, r := range rs {
			if r.Match(g, mp) {
				return r.Rewrite(g, mp, gmp, gmq)
			}
		}
	}
	return g, false
}

var Rules = map[int][]func(context.Context) rule.Rule{
	op.Restrict:  []func(context.Context) rule.Rule{rule0.New},
	op.Summarize: []func(context.Context) rule.Rule{rule100.New, rule101.New},
	op.Group:     []func(context.Context) rule.Rule{rule200.New, rule201.New},
}
