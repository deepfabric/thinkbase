package rule100

import (
	"github.com/deepfabric/thinkbase/pkg/vm/op"
)

func New() *rule {
	return &rule{}
}

func (r *rule) Match(o op.OP, mp map[string]op.OP) bool {
	return o.Operate() == op.SetUnion && r.findSortOp(o, mp) == nil
}

func (r *rule) Rewrite(o op.OP, mp map[string]op.OP) (op.OP, bool) {
	left := r.removeSortOp(o.Children()[0], mp)
	right := r.removeSortOp(o.Children()[1], mp)
	if n, ok := o.(op.SetUnionOP); ok {
		if no := n.NewHashUnion(left, right); no.Cost() < o.Cost() {
			if parent, ok := mp[o.String()]; ok {
				children := parent.Children()
				for i, child := range children {
					if child == o {
						parent.SetChild(no, i)
						break
					}
				}
			} else {
				mp[""] = no
			}
			return no, true
		}
	}
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
