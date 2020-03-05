package projection

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/extend/overload"
	aprojection "github.com/deepfabric/thinkbase/pkg/algebra/projection"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/exec/testunit"
)

func TestProjection(t *testing.T) {
	r := newTestRelation()
	{
		fmt.Printf("r:\n%s\n", r)
	}
	attrs := []*aprojection.Attribute{}
	{
		a, err := extend.NewAttribute("a", r)
		if err != nil {
			log.Fatal(err)
		}
		e := &extend.BinaryExtend{
			Op:    overload.Mult,
			Left:  a,
			Right: value.NewInt(5),
		}
		attrs = append(attrs, &aprojection.Attribute{Alias: "A", E: e})
	}
	{
		a, err := extend.NewAttribute("b", r)
		if err != nil {
			log.Fatal(err)
		}
		attrs = append(attrs, &aprojection.Attribute{Alias: "B", E: a})
	}
	us, err := testunit.NewProjection(3, attrs, r)
	if err != nil {
		log.Fatal(err)
	}
	pr, err := New(us).Projection()
	if err != nil {
		log.Fatal(err)
	}
	{
		fmt.Printf("pr:\n%s\n", pr)
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
