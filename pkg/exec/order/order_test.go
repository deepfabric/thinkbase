package order

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/exec/testunit"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
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
	attrs := make([]*relation.AttributeMetadata, 2)
	attrs[0] = &relation.AttributeMetadata{
		Name:  "a",
		Types: make(map[int32]int),
	}
	attrs[1] = &relation.AttributeMetadata{
		Name:  "b",
		Types: make(map[int32]int),
	}
	r := relation.New("A", nil, attrs)
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

func newCompare(isNub bool, descs []bool, attrs []string, md []*relation.AttributeMetadata) func(value.Tuple, value.Tuple) bool {
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

func getAttributeIndex(name string, md []*relation.AttributeMetadata) int {
	for i, a := range md {
		if a.Name == name {
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
