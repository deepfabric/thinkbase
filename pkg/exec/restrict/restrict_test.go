package restrict

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/exec/testunit"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
)

func TestRestrict(t *testing.T) {
	r := newTestRelation()
	{
		fmt.Printf("r:\n%s\n", r)
	}
	a, err := extend.NewAttribute("a", r)
	if err != nil {
		log.Fatal(err)
	}
	e1 := &extend.UnaryExtend{
		E:  a,
		Op: overload.Typeof,
	}
	e := &extend.BinaryExtend{
		Op:    overload.EQ,
		Left:  e1,
		Right: value.NewString("int"),
	}
	us, err := testunit.NewRestrict(3, e, r)
	if err != nil {
		log.Fatal(err)
	}
	rr, err := New(us).Restrict()
	if err != nil {
		log.Fatal(err)
	}
	{
		fmt.Printf("rr:\n%s\n", rr)
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
