package summarize

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
	aoverload "github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/types"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func TestSummarize(t *testing.T) {
	{
		r := newRelation()
		fmt.Printf("%s\n", r.DataString())
	}
	c := context.New(context.NewConfig("tom"), nil, nil)
	{
		var es []*projection.Extend

		prev := newSummarize(c)
		es = append(es, &projection.Extend{
			E: &extend.Attribute{"A"},
		})
		es = append(es, &projection.Extend{
			E: &extend.Attribute{"B"},
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
			mp, err := n.GetAttributes([]string{"A", "B"}, bs)
			if err != nil {
				log.Fatal(err)
			}
			if len(mp["A"]) == 0 {
				break
			}
			fmt.Printf("A = %v\n", mp["A"])
			fmt.Printf("B = %v\n", mp["B"])
		}
	}
}

func newSummarize(c context.Context) op.OP {
	var es []*Extend

	es = append(es, &Extend{
		Name:  "a",
		Alias: "A",
		Typ:   types.T_any,
		Op:    aoverload.Avg,
	})
	es = append(es, &Extend{
		Name:  "b",
		Alias: "B",
		Typ:   types.T_any,
		Op:    aoverload.Max,
	})
	prev := newRestrict(c)
	return New(prev, es, c)
}

func newRestrict(c context.Context) op.OP {
	r := newRelation()
	e := &extend.BinaryExtend{
		Op:    overload.GT,
		Left:  &extend.Attribute{"a"},
		Right: value.NewInt(1),
	}
	return restrict.New(r, e, c)
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
