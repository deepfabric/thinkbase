package rule100

import (
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

func New() *rule {
	return &rule{}
}

func (r *rule) Match(o op.OP) bool {
	return o.Operate() == op.SetUnion
}

func (r *rule) Rewrite(o op.OP, mp map[string]op.OP) (op.OP, bool) {
	if sp := r.findSortOp(o, mp); sp == nil {
		return r.rewriteWithOutOrder(o, mp)
	} else {
		return r.rewriteWithOrder(o, sp, mp)
	}
}

func (r *rule) rewriteWithOutOrder(o op.OP, mp map[string]op.OP) (op.OP, bool) {
	left := r.removeSortOp(o.Children()[0], mp)
	right := r.removeSortOp(o.Children()[1], mp)
	if no := o.(op.SetUnionOP).NewHashUnion(left, right); no.Cost() < o.Cost() {
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

func (r *rule) rewriteWithOrder(o, _ op.OP, mp map[string]op.OP) (op.OP, bool) {
	return o, false
}

func (r *rule) removeSortOp(o op.OP, mp map[string]op.OP) op.OP {
	p := o
	for {
		children := p.Children()
		if len(children) > 1 || len(children) < 1 {
			return o
		}
		if children[0].Operate() == op.Order {
			p.SetChild(children[0].Children()[0], 0)
		}
		p = children[0]
	}
}

func (r *rule) findSortOp(o op.OP, mp map[string]op.OP) op.OP {
	for {
		if parent, ok := mp[o.String()]; ok {
			if len(parent.Children()) > 1 {
				return nil
			}
			if parent.Operate() == op.Order {
				return parent
			}
			o = parent
		} else {
			return nil
		}
	}
}
