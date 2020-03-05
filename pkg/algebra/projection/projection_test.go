package projection

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

func TestProjection(t *testing.T) {
	r := newTestRelation()
	{
		fmt.Printf("r:\n%s\n", r)
	}
	attrs := []*Attribute{}
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
		attrs = append(attrs, &Attribute{Alias: "A", E: e})
	}
	{
		a, err := extend.NewAttribute("b", r)
		if err != nil {
			log.Fatal(err)
		}
		attrs = append(attrs, &Attribute{Alias: "B", E: a})
	}
	p := New(r, attrs)
	pr, err := p.Projection()
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
