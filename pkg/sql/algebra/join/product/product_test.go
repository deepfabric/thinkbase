package product

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
)

func TestProduct(t *testing.T) {
	a := newTestRelation0()
	b := newTestRelation1()
	{
		fmt.Printf("a:\n%s\n", a)
	}
	{
		fmt.Printf("b:\n%s\n", b)
	}
	{
		r, err := New(a, b).Join()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("r:\n%s\n", r)
	}
}

func newTestRelation0() relation.Relation {
	attrs := make([]*relation.AttributeMetadata, 3)
	attrs[0] = &relation.AttributeMetadata{
		Name:  "a",
		Types: make(map[int32]int),
	}
	attrs[1] = &relation.AttributeMetadata{
		Name:  "b",
		Types: make(map[int32]int),
	}
	attrs[2] = &relation.AttributeMetadata{
		Name:  "c",
		Types: make(map[int32]int),
	}

	r := relation.New("A", nil, attrs)
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
	attrs := make([]*relation.AttributeMetadata, 2)
	attrs[0] = &relation.AttributeMetadata{
		Name:  "b",
		Types: make(map[int32]int),
	}
	attrs[1] = &relation.AttributeMetadata{
		Name:  "d",
		Types: make(map[int32]int),
	}
	r := relation.New("B", nil, attrs)
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
