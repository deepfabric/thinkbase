package right

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
)

func TestRight(t *testing.T) {
	a := newTestRelation0()
	b := newTestRelation1()
	{
		fmt.Printf("a:\n%s\n", a)
	}
	{
		fmt.Printf("b:\n%s\n", b)
	}
	{
		ea, err := extend.NewAttribute("b", a)
		if err != nil {
			log.Fatal(err)
		}
		eb, err := extend.NewAttribute("b", b)
		if err != nil {
			log.Fatal(err)
		}
		e := &extend.BinaryExtend{
			Op:    overload.GT,
			Left:  ea,
			Right: eb,
		}
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
	attrs = append(attrs, "c")

	r := mem.New("A", attrs)
	{
		var t value.Tuple

		t = append(t, value.NewInt(1))
		t = append(t, value.NewString("a"))
		t = append(t, value.NewString("d"))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewInt(3))
		t = append(t, value.NewString("c"))
		t = append(t, value.NewString("c"))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewInt(4))
		t = append(t, value.NewString("d"))
		t = append(t, value.NewString("f"))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewInt(5))
		t = append(t, value.NewString("d"))
		t = append(t, value.NewString("b"))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewInt(6))
		t = append(t, value.NewString("e"))
		t = append(t, value.NewString("f"))
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
		t = append(t, value.NewInt(100))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewString("b"))
		t = append(t, value.NewInt(300))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewString("c"))
		t = append(t, value.NewInt(400))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewString("d"))
		t = append(t, value.NewInt(200))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewString("e"))
		t = append(t, value.NewInt(150))
		r.AddTuple(t)
	}
	return r
}
