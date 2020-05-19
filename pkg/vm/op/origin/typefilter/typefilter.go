package typefilter

import (
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/restrict"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func New(o restrict.RestrictOP) *Typefilter {
	return &Typefilter{o}
}

func (n *Typefilter) Extend() extend.Extend {
	return n.o.Extend()
}

func (n *Typefilter) Size() float64 {
	return n.o.Size()
}

func (n *Typefilter) Cost() float64 {
	return n.o.Cost()
}

func (n *Typefilter) Dup() op.OP {
	return &Typefilter{n.o}
}

func (n *Typefilter) Operate() int {
	return n.o.Operate()
}

func (n *Typefilter) Children() []op.OP {
	return n.o.Children()
}

func (n *Typefilter) SetChild(o op.OP, idx int) {
	n.o.SetChild(o, idx)
}

func (n *Typefilter) IsOrdered() bool {
	return n.o.IsOrdered()
}

func (n *Typefilter) String() string {
	return n.o.String()
}

func (n *Typefilter) Name() (string, error) {
	return n.o.Name()
}

func (n *Typefilter) AttributeList() ([]string, error) {
	return n.o.AttributeList()
}

func (n *Typefilter) GetAttributes(attrs []string, limit int) (map[string]value.Array, error) {
	return n.o.GetAttributes(attrs, limit)
}
