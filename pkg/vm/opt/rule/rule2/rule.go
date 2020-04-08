package rule2

import (
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

func New() *rule {
	return &rule{}
}

func (r *rule) Match(o op.OP) bool {
	chidren := o.Children()
	return o.Operate() == op.Restrict && len(chidren) > 0 && chidren[0].Operate() == op.Projection
}

func (r *rule) Rewrite(o op.OP, mp map[string]op.OP) (op.OP, bool) {
	if no := r.newbranch(o); no.Cost() < o.Cost() {
		if parent, ok := mp[o.String()]; ok {
			children := parent.Children()
			for i, child := range children {
				if child == o {
					parent.SetChild(no, i)
					break
				}
			}
		}
		return no, true
	}
	return o, false
}

func (r *rule) newbranch(o op.OP) op.OP {
	no := o.Children()[0].Dup()
	no.SetChild(o.Dup(), 0)
	children := o.Children()[0].Children()
	for i, child := range children {
		no.Children()[0].SetChild(child, i)
	}
	return no
}
