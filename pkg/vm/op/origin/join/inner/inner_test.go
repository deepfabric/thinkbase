package inner

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/context/testContext"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func TestInner(t *testing.T) {
	{
		r := newRelation0()
		fmt.Printf("%s\n", r.DataString())
	}
	{
		r := newRelation1()
		fmt.Printf("%s\n", r.DataString())
	}
	{
		el := &extend.BinaryExtend{
			Op:    overload.EQ,
			Left:  &extend.Attribute{"a"},
			Right: value.NewInt(1),
		}
		er := &extend.BinaryExtend{
			Op:    overload.GT,
			Left:  &extend.Attribute{"B.b"},
			Right: value.NewString("c"),
		}
		e := &extend.BinaryExtend{
			Op:    overload.And,
			Left:  el,
			Right: er,
		}
		n := New(newRelation0(), newRelation1(), e, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
		{
			fmt.Printf("%s\n", n)
		}
		{
			name, err := n.Name()
			fmt.Printf("%v, %v\n", name, err)
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
		el := &extend.BinaryExtend{
			Op:    overload.EQ,
			Left:  &extend.Attribute{"a"},
			Right: value.NewInt(1),
		}
		er := &extend.BinaryExtend{
			Op:    overload.GT,
			Left:  &extend.Attribute{"B.b"},
			Right: value.NewString("c"),
		}
		e := &extend.BinaryExtend{
			Op:    overload.And,
			Left:  el,
			Right: er,
		}
		n := New(newRelation0(), newRelation1(), e, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
		{
			fmt.Printf("%s\n", n)
		}
		{
			name, err := n.Name()
			fmt.Printf("%v, %v\n", name, err)
		}
		{
			attrs, err := n.AttributeList()
			fmt.Printf("%v, %v\n", attrs, err)
		}
		for {
			mp, err := n.GetAttributes([]string{"c", "d"}, 1024*1024)
			if err != nil {
				log.Fatal(err)
			}
			if len(mp["c"]) == 0 {
				break
			}
			fmt.Printf("c = %v\n", mp["c"])
			fmt.Printf("d = %v\n", mp["d"])
		}

	}
}

func newRelation0() relation.Relation {
	var attrs []string

	attrs = append(attrs, "a")
	attrs = append(attrs, "b")
	attrs = append(attrs, "c")
	r := mem.New("A", attrs)
	{
		var t value.Array

		t = append(t, value.NewInt(1))
		t = append(t, value.NewString("a"))
		t = append(t, value.NewString("d"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewInt(3))
		t = append(t, value.NewString("c"))
		t = append(t, value.NewString("c"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewInt(4))
		t = append(t, value.NewString("d"))
		t = append(t, value.NewString("f"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewInt(5))
		t = append(t, value.NewString("d"))
		t = append(t, value.NewString("b"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewInt(6))
		t = append(t, value.NewString("e"))
		t = append(t, value.NewString("f"))
		r.AddTuples([]value.Array{t})
	}
	return r
}

func newRelation1() relation.Relation {
	var attrs []string

	attrs = append(attrs, "b")
	attrs = append(attrs, "d")
	r := mem.New("B", attrs)
	{
		var t value.Array

		t = append(t, value.NewString("a"))
		t = append(t, value.NewInt(100))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("b"))
		t = append(t, value.NewInt(300))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("c"))
		t = append(t, value.NewInt(400))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("d"))
		t = append(t, value.NewInt(200))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewString("e"))
		t = append(t, value.NewInt(150))
		r.AddTuples([]value.Array{t})
	}
	return r
}
