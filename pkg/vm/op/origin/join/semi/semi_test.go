package semi

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/context/testContext"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func TestSemi(t *testing.T) {
	{
		r := newRelation0()
		fmt.Printf("%s\n", r)
	}
	{
		r := newRelation1()
		fmt.Printf("%s\n", r)
	}
	{
		n := New(newRelation0(), newRelation1(), testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
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
		n := New(newRelation0(), newRelation1(), testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024))
		{
			name, err := n.Name()
			fmt.Printf("%v, %v\n", name, err)
		}
		{
			attrs, err := n.AttributeList()
			fmt.Printf("%v, %v\n", attrs, err)
		}
		for {
			mp, err := n.GetAttributes([]string{"b", "c"}, 1024*1024)
			if err != nil {
				log.Fatal(err)
			}
			if len(mp["b"]) == 0 {
				break
			}
			fmt.Printf("b = %v\n", mp["b"])
			fmt.Printf("c = %v\n", mp["c"])
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

		t = append(t, value.NewString("1"))
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
	{
		var t value.Array

		t = append(t, value.NewInt(7))
		t = append(t, value.NewString("f"))
		t = append(t, value.NewString("g"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewInt(8))
		t = append(t, value.NewString("g"))
		t = append(t, value.NewString("h"))
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
