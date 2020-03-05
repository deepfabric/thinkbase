package order

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/exec/testunit"
)

func TestOrder(t *testing.T) {
	r := newTestRelation()
	{
		fmt.Printf("%s\n", r)
	}
	{
		cmp := newCompare(false, []bool{false}, []string{"a"}, r.Metadata())
		us, err := testunit.NewOrder(2, false, []bool{false}, []string{"a"}, r)
		if err != nil {
			log.Fatal(err)
		}
		rr, err := New(us, cmp).Order()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", rr)
	}
	{
		cmp := newCompare(false, []bool{true}, []string{"a"}, r.Metadata())
		us, err := testunit.NewOrder(2, false, []bool{true}, []string{"a"}, r)
		if err != nil {
			log.Fatal(err)
		}
		rr, err := New(us, cmp).Order()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", rr)
	}
}

func newTestRelation() relation.Relation {
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
	{
		var t value.Tuple

		t = append(t, value.NewFloat(3.1))
		t = append(t, value.NewInt(3))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewInt(1))
		t = append(t, value.NewString("x"))
		r.AddTuple(t)
	}
	return r
}

func newCompare(isNub bool, descs []bool, attrs []string, md []string) func(value.Tuple, value.Tuple) bool {
	var is []int

	for _, attr := range attrs {
		is = append(is, getAttributeIndex(attr, md))
	}
	return func(x, y value.Tuple) bool {
		for idx, i := range is {
			if i >= 0 {
				if r := int(x[i].ResolvedType().Oid - y[i].ResolvedType().Oid); r != 0 {
					return less(descs[idx], r)
				}
				if r := x[i].Compare(y[i]); r != 0 {
					return less(descs[idx], r)
				}
			}
		}
		return false
	}
}

func getAttributeIndex(attr string, md []string) int {
	for i, a := range md {
		if a == attr {
			return i
		}
	}
	return -1
}

func less(desc bool, r int) bool {
	if desc {
		return r > 0
	}
	return r < 0
}
