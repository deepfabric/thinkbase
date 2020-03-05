package inner

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
)

func TestInner(t *testing.T) {
	a := newTestRelation0()
	b := newTestRelation1()
	{
		fmt.Printf("a:\n%s\n", a)
	}
	{
		fmt.Printf("b:\n%s\n", b)
	}
	{
		e := value.NewBool(true)

		r, err := New(e, a, b).Join()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("r:\n%s\n", r)
	}
}

func newTestRelation0() relation.Relation {
	var attrs []string

	attrs = append(attrs, "a")
	attrs = append(attrs, "b")
	r := mem.New("A", attrs)
	{
		var t value.Tuple

		t = append(t, value.NewString("a"))
		t = append(t, value.NewInt(1))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewString("b"))
		t = append(t, value.NewInt(2))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewString("c"))
		t = append(t, value.NewInt(3))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewString("d"))
		t = append(t, value.NewInt(4))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewString("e"))
		t = append(t, value.NewInt(5))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewString("f"))
		t = append(t, value.NewInt(6))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewString("g"))
		t = append(t, value.NewInt(7))
		r.AddTuple(t)
	}
	return r
}

func newTestRelation1() relation.Relation {
	var attrs []string

	attrs = append(attrs, "b")
	attrs = append(attrs, "d")
	r := mem.New("B", attrs)
	{
		var t value.Tuple

		t = append(t, value.NewString("a"))
		t = append(t, value.NewString("name1"))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewString("a"))
		t = append(t, value.NewString("name1"))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewString("b"))
		t = append(t, value.NewString("name2"))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewString("c"))
		t = append(t, value.NewString("name3"))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewString("d"))
		t = append(t, value.NewString("name4"))
		r.AddTuple(t)
	}
	return r
}
