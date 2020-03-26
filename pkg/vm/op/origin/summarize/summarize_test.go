package summarize

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
	aoverload "github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func TestSummarize(t *testing.T) {
	{
		r := newRelation()
		fmt.Printf("%s\n", r)
	}
	{
		var es []*projection.Extend

		prev := newSummarize()
		es = append(es, &projection.Extend{
			E: &extend.Attribute{"A"},
		})
		es = append(es, &projection.Extend{
			E: &extend.Attribute{"B"},
		})
		n := projection.New(prev, es, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
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
		var es []*projection.Extend

		prev := newSummarize()
		es = append(es, &projection.Extend{
			E: &extend.Attribute{"A"},
		})
		es = append(es, &projection.Extend{
			E: &extend.Attribute{"B"},
		})
		n := projection.New(prev, es, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
		{
			attrs, err := n.AttributeList()
			fmt.Printf("%v, %v\n", attrs, err)
		}
		for {
			mp, err := n.GetAttributes([]string{"A"}, 1024*1024)
			if err != nil {
				log.Fatal(err)
			}
			if len(mp["A"]) == 0 {
				break
			}
			fmt.Printf("A = %v\n", mp["A"])
		}
	}
}

func newSummarize() op.OP {
	var es []*Extend

	es = append(es, &Extend{
		Name:  "a",
		Alias: "A",
		Op:    aoverload.Avg,
	})
	es = append(es, &Extend{
		Name:  "b",
		Alias: "B",
		Op:    aoverload.Max,
	})
	prev := newRestrict()
	return New(prev, es, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
}

func newRestrict() op.OP {
	r := newRelation()
	e := &extend.BinaryExtend{
		Op:    overload.GT,
		Left:  &extend.Attribute{"a"},
		Right: value.NewInt(1),
	}
	return restrict.New(r, e, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
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
