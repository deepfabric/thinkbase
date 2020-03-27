package a2t

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/context/testContext"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/projection"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/restrict"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func TestA2t(t *testing.T) {
	{
		r := newRelation()
		fmt.Printf("%s\n", r)
	}
	{
		r := newRelation()
		e := &extend.BinaryExtend{
			Op:    overload.GT,
			Left:  &extend.Attribute{"a"},
			Right: value.NewInt(1),
		}
		n := restrict.New(r, e, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
		{
			attrs, err := n.AttributeList()
			fmt.Printf("%v, %v\n", attrs, err)
		}
		for {
			ts, err := n.GetTuples(1024 * 1024)
			if err != nil {
				log.Fatal(err)
			}
			if len(ts) == 0 {
				break
			}
			for i, t := range ts {
				fmt.Printf("[%v] = %v\n", i, t)
			}
		}
	}
	{
		r := newRelation()
		e := &extend.BinaryExtend{
			Op:    overload.GT,
			Left:  &extend.Attribute{"a"},
			Right: value.NewInt(1),
		}
		n := restrict.New(r, e, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
		{
			attrs, err := n.AttributeList()
			fmt.Printf("%v, %v\n", attrs, err)
		}
		for {
			mp, err := n.GetAttributes([]string{"a", "b"}, 1024*1024)
			if err != nil {
				log.Fatal(err)
			}
			if len(mp["a"]) == 0 {
				break
			}
			fmt.Printf("a = %v\n", mp["a"])
			fmt.Printf("b = %v\n", mp["b"])
		}
	}
}

func newA2t() op.OP {
	return New(newProjection(), testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
}

func newProjection() op.OP {
	var es []*projection.Extend

	prev := newRelation()
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
	return projection.New(prev, es, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))

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
