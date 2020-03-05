package union

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/exec/testunit"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
)

func TestUnion(t *testing.T) {
	a := newTestRelation0()
	b := newTestRelation1()
	{
		fmt.Printf("a:\n%s\n", a)
	}
	{
		fmt.Printf("b:\n%s\n", b)
	}
	{
		us, err := testunit.New(3, unit.Union, a, b)
		if err != nil {
			log.Fatal(err)
		}
		r, err := New(true, us).Union()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("r:\n%s\n", r)
	}
	{
		us, err := testunit.New(3, unit.Union, a, b)
		if err != nil {
			log.Fatal(err)
		}
		r, err := New(false, us).Union()
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

		t = append(t, value.NewInt(1))
		t = append(t, value.NewString("x"))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewInt(3))
		t = append(t, value.NewString("y"))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewInt(2))
		t = append(t, value.NewString("m"))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewFloat(3.1))
		t = append(t, value.NewInt(3))
		r.AddTuple(t)
	}

	return r
}

func newTestRelation1() relation.Relation {
	var attrs []string

	attrs = append(attrs, "a")
	attrs = append(attrs, "b")
	r := mem.New("B", attrs)
	{
		var t value.Tuple

		t = append(t, value.NewInt(100))
		t = append(t, value.NewInt(3))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewString("xxx"))
		t = append(t, value.NewFloat(3.1))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewFloat(3.1))
		t = append(t, value.NewInt(3))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewFloat(3.1))
		t = append(t, value.NewInt(3))
		r.AddTuple(t)
	}
	return r
}
