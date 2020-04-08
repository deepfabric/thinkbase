package projection

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/context/testContext"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/extend/overload"
	oprojection "github.com/deepfabric/thinkbase/pkg/vm/op/origin/projection"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func TestProjection(t *testing.T) {
	{
		r := newRelation()
		fmt.Printf("%s\n", r.DataString())
	}
	{
		var es []*oprojection.Extend

		r := newRelation()
		es = append(es, &oprojection.Extend{
			Alias: "C",
			E:     &extend.Attribute{"a"},
		})
		es = append(es, &oprojection.Extend{
			Alias: "A",
			E: &extend.UnaryExtend{
				Op: overload.Typeof,
				E:  &extend.Attribute{"b"},
			},
		})
		n := New(testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024), r, es)
		{
			fmt.Printf("%s\n", n)
		}
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
		var es []*oprojection.Extend

		r := newRelation()
		es = append(es, &oprojection.Extend{
			Alias: "C",
			E:     &extend.Attribute{"a"},
		})
		es = append(es, &oprojection.Extend{
			Alias: "A",
			E: &extend.UnaryExtend{
				Op: overload.Typeof,
				E:  &extend.Attribute{"b"},
			},
		})
		n := New(testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024), r, es)
		{
			fmt.Printf("%s\n", n)
		}
		{
			attrs, err := n.AttributeList()
			fmt.Printf("%v, %v\n", attrs, err)
		}
		for {
			mp, err := n.GetAttributes([]string{"C", "A"}, 1024*1024)
			if err != nil {
				log.Fatal(err)
			}
			if len(mp["C"]) == 0 {
				break
			}
			fmt.Printf("C = %v\n", mp["C"])
			fmt.Printf("A = %v\n", mp["A"])
		}
	}
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
