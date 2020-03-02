package order

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
)

func TestOrder(t *testing.T) {
	r := newTestRelation()
	{
		fmt.Printf("%s\n", r)
	}
	{
		rr, err := New(false, []bool{false}, []string{"a"}, r).Order()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", rr)
	}
	{
		rr, err := New(false, []bool{true}, []string{"a"}, r).Order()
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
