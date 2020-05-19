package group

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/projection"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/restrict"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize"
	aoverload "github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func TestGroup(t *testing.T) {
	{
		r := newRelation()
		fmt.Printf("%s\n", r.DataString())
	}
	c := context.New(context.NewConfig("tom"), nil, nil)
	{
		var es []*projection.Extend

		prev := newGroup(c)
		es = append(es, &projection.Extend{
			E: &extend.Attribute{"a"},
		})
		es = append(es, &projection.Extend{
			E: &extend.Attribute{"B"},
		})
		es = append(es, &projection.Extend{
			E: &extend.Attribute{"C"},
		})
		n := projection.New(prev, es, c)
		{
			fmt.Printf("%s\n", n)
		}
		{
			attrs, err := n.AttributeList()
			fmt.Printf("%v, %v\n", attrs, err)
		}
		bs := c.BlockSize()
		for {
			mp, err := n.GetAttributes([]string{"a", "B", "C"}, bs)
			if err != nil {
				log.Fatal(err)
			}
			if len(mp["B"]) == 0 {
				break
			}
			fmt.Printf("a = %v\n", mp["a"])
			fmt.Printf("B = %v\n", mp["B"])
			fmt.Printf("C = %v\n", mp["C"])
		}
	}
}

func newGroup(c context.Context) op.OP {
	var es []*summarize.Extend

	es = append(es, &summarize.Extend{
		Name:  "b",
		Alias: "B",
		Op:    aoverload.Avg,
	})
	es = append(es, &summarize.Extend{
		Name:  "c",
		Alias: "C",
		Op:    aoverload.Max,
	})
	es = append(es, &summarize.Extend{
		Name:  "b",
		Alias: "sum(b)",
		Op:    aoverload.Sum,
	})
	prev := newRestrict(c)
	e := &extend.BinaryExtend{
		Op:    overload.GT,
		Right: value.NewFloat(2.0),
		Left:  &extend.Attribute{"sum(b)"},
	}
	return New(prev, e, []string{"a"}, es, c)
}

func newRestrict(c context.Context) op.OP {
	r := newRelation()
	e := &extend.BinaryExtend{
		Op: overload.EQ,
		Left: &extend.UnaryExtend{
			Op: overload.Typeof,
			E:  &extend.Attribute{"a"},
		},
		Right: value.NewString("string"),
	}
	return restrict.New(r, e, c)
}

func newRelation() relation.Relation {
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
	return r
}
