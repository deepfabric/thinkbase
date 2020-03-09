package match

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
)

func TestMatch(t *testing.T) {
	ct := context.New()
	a := newTestRelation0(ct)
	b := newTestRelation1(ct)
	{
		fmt.Printf("a:\n%s\n", a)
	}
	{
		fmt.Printf("b:\n%s\n", b)
	}
	{
		r, err := New(ct, a, b).Join()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("r:\n%s\n", r)
	}
}

func newTestRelation0(c context.Context) relation.Relation {
	var attrs []string

	attrs = append(attrs, "a")
	attrs = append(attrs, "b")
	r := mem.New("A", attrs, c)
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

func newTestRelation1(c context.Context) relation.Relation {
	var attrs []string

	attrs = append(attrs, "a")
	attrs = append(attrs, "c")
	r := mem.New("B", attrs, c)
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
