package main

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/context/testContext"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/order"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/projection"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/restrict"
	"github.com/deepfabric/thinkbase/pkg/vm/opt"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func main() {
	var es []*projection.Extend

	prev := newRestrict()
	es = append(es, &projection.Extend{
		E: &extend.Attribute{"a"},
	})
	es = append(es, &projection.Extend{
		Alias: "A",
		E: &extend.UnaryExtend{
			Op: overload.Typeof,
			E:  &extend.Attribute{"b"},
		},
	})
	n := projection.New(prev, es, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
	fmt.Printf("%s\n", n)
	no := opt.New(n).Optimize()
	fmt.Printf("%s\n", no)
}

func newOrder() op.OP {
	return order.New(newRelation(), []bool{true, false}, []string{"a", "b"}, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
}

func newRestrict() op.OP {
	e := &extend.BinaryExtend{
		Op:    overload.GT,
		Left:  &extend.Attribute{"a"},
		Right: value.NewInt(1),
	}
	return restrict.New(newOrder(), e, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
}

func newRelation() relation.Relation {
	var attrs []string

	attrs = append(attrs, "a")
	attrs = append(attrs, "b")
	r := mem.New("A", attrs)
	{
		var t value.Array

		t = append(t, value.NewInt(1))
		t = append(t, value.NewString("x"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewInt(3))
		t = append(t, value.NewString("y"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewInt(2))
		t = append(t, value.NewString("m"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewFloat(3.1))
		t = append(t, value.NewInt(3))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewFloat(3.1))
		t = append(t, value.NewInt(3))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewInt(1))
		t = append(t, value.NewString("x"))
		r.AddTuples([]value.Array{t})
	}
	return r
}
