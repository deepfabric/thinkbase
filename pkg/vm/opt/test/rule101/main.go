package main

import (
	"fmt"

	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/restrict"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize"
	aoverload "github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/opt"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func main() {
	c := context.New(context.NewConfig("tom"), nil, nil)
	n := newSummarize(newRestrict(c), c)
	fmt.Printf("%s\n", n)
	no := opt.New(n, c).Optimize()
	fmt.Printf("%s\n", no)
}

func newSummarize(prev op.OP, c context.Context) op.OP {
	var es []*summarize.Extend

	es = append(es, &summarize.Extend{
		Name:  "a",
		Alias: "A",
		Op:    aoverload.Avg,
	})
	es = append(es, &summarize.Extend{
		Name:  "b",
		Alias: "B",
		Op:    aoverload.Min,
	})
	return summarize.New(prev, es, c)
}

func newRestrict(c context.Context) op.OP {
	e0 := &extend.UnaryExtend{
		Op: overload.Typeof,
		E:  &extend.Attribute{"a"},
	}
	e1 := &extend.BinaryExtend{
		Op:    overload.EQ,
		Left:  e0,
		Right: value.NewString("int"),
	}
	e2 := &extend.UnaryExtend{
		Op: overload.Typeof,
		E:  &extend.Attribute{"b"},
	}
	e3 := &extend.BinaryExtend{
		Op:    overload.EQ,
		Left:  e2,
		Right: value.NewString("string"),
	}
	e4 := &extend.BinaryExtend{
		Op:    overload.And,
		Left:  e1,
		Right: e3,
	}
	e5 := &extend.BinaryExtend{
		Op:    overload.LT,
		Left:  &extend.Attribute{"a"},
		Right: value.NewInt(10),
	}
	e6 := &extend.BinaryExtend{
		Op:    overload.And,
		Left:  e4,
		Right: e5,
	}
	return restrict.New(newRelation(), e6, c)
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
