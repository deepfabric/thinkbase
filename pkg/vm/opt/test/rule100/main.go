package main

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/context/testContext"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/nub"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/order"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/projection"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/restrict"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/set/union"
	"github.com/deepfabric/thinkbase/pkg/vm/opt"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func main() {
	n := union.New(newRestrict0(), newRestrict1(), testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
	fmt.Printf("%s\n", n)
	no := opt.New(n).Optimize()
	fmt.Printf("%s\n", no)
}

func newProjection0() op.OP {
	var es []*projection.Extend

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
	return projection.New(newOrder0(), es, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
}

func newProjection1() op.OP {
	var es []*projection.Extend

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
	return projection.New(newOrder1(), es, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
}

func newNub0() op.OP {
	return nub.New(newProjection0(), []string{"a"}, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
}

func newNub1() op.OP {
	return nub.New(newProjection1(), []string{"a"}, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
}

func newOrder0() op.OP {
	return order.New(newRelation0(), []bool{true, false}, []string{"a", "b"}, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
}

func newOrder1() op.OP {
	return order.New(newRelation1(), []bool{true, false}, []string{"a", "b"}, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
}

func newRestrict0() op.OP {
	e := &extend.BinaryExtend{
		Op:    overload.GT,
		Left:  &extend.Attribute{"a"},
		Right: value.NewInt(1),
	}
	return restrict.New(newNub0(), e, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
}

func newRestrict1() op.OP {
	e := &extend.BinaryExtend{
		Op:    overload.GT,
		Left:  &extend.Attribute{"a"},
		Right: value.NewInt(1),
	}
	return restrict.New(newNub1(), e, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
}

func newRelation0() relation.Relation {
	var attrs []string

	attrs = append(attrs, "a")
	attrs = append(attrs, "b")
	attrs = append(attrs, "c")
	r := mem.New("A", attrs)
	{
		var t value.Array

		t = append(t, value.NewString("a"))
		t = append(t, value.NewInt(1))
		t = append(t, value.NewString("x"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("a"))
		t = append(t, value.NewInt(3))
		t = append(t, value.NewString("y"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("b"))
		t = append(t, value.NewInt(2))
		t = append(t, value.NewString("m"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("c"))
		t = append(t, value.NewFloat(3.1))
		t = append(t, value.NewInt(3))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("c"))
		t = append(t, value.NewInt(1))
		t = append(t, value.NewString("x"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("d"))
		t = append(t, value.NewFloat(3.1))
		t = append(t, value.NewInt(3))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("c"))
		t = append(t, value.NewInt(1))
		t = append(t, value.NewString("x"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewInt(100))
		t = append(t, value.NewInt(3))
		t = append(t, value.NewString("x"))
		r.AddTuples([]value.Array{t})
	}
	return r
}

func newRelation1() relation.Relation {
	var attrs []string

	attrs = append(attrs, "a")
	attrs = append(attrs, "b")
	attrs = append(attrs, "c")
	r := mem.New("B", attrs)
	{
		var t value.Array

		t = append(t, value.NewInt(100))
		t = append(t, value.NewInt(3))
		t = append(t, value.NewString("x"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("xxx"))
		t = append(t, value.NewFloat(3.1))
		t = append(t, value.NewString("yyy"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("b"))
		t = append(t, value.NewInt(2))
		t = append(t, value.NewString("m"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("c"))
		t = append(t, value.NewFloat(3.1))
		t = append(t, value.NewInt(3))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("d"))
		t = append(t, value.NewFloat(3.1))
		t = append(t, value.NewInt(3))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("c"))
		t = append(t, value.NewInt(1))
		t = append(t, value.NewString("x"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewInt(100))
		t = append(t, value.NewInt(3))
		t = append(t, value.NewString("x"))
		r.AddTuples([]value.Array{t})
	}
	return r
}
