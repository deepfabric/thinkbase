package summarize

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/sql/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/projection"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
)

func TestSummarize(t *testing.T) {
	r := newSummarize()
	attrs := []*projection.Attribute{}
	{
		a, err := extend.NewAttribute("A", r)
		if err != nil {
			log.Fatal(err)
		}
		attrs = append(attrs, &projection.Attribute{E: a})
	}
	{
		a, err := extend.NewAttribute("a", r)
		if err != nil {
			log.Fatal(err)
		}
		attrs = append(attrs, &projection.Attribute{E: a})
	}
	{
		a, err := extend.NewAttribute("B", r)
		if err != nil {
			log.Fatal(err)
		}
		attrs = append(attrs, &projection.Attribute{E: a})
	}
	{
		a, err := extend.NewAttribute("C", r)
		if err != nil {
			log.Fatal(err)
		}
		attrs = append(attrs, &projection.Attribute{E: a})
	}
	p := projection.New(r, attrs)
	pr, err := p.Projection()
	if err != nil {
		log.Fatal(err)
	}
	{
		fmt.Printf("pr:\n%s\n", pr)
	}
}

func newSummarize() relation.Relation {
	r := newTestRelation()
	{
		fmt.Printf("r:\n%s\n", r)
	}
	ops := []int{}
	gs := []string{}
	attrs := []*Attribute{}
	{
		gs = append(gs, "b")
	}
	{
		ops = append(ops, overload.Avg)
		attrs = append(attrs, &Attribute{Name: "a", Alias: "A"})
	}
	{
		ops = append(ops, overload.Sum)
		attrs = append(attrs, &Attribute{Name: "a", Alias: "B"})
	}
	{
		ops = append(ops, overload.Max)
		attrs = append(attrs, &Attribute{Name: "b", Alias: "C"})
	}
	s := New(ops, gs, attrs, r)
	sr, err := s.Summarize()
	if err != nil {
		log.Fatal(err)
	}
	return sr
}

func newTestRelation() relation.Relation {
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
	{
		var t value.Tuple

		t = append(t, value.NewInt(7))
		t = append(t, value.NewString("e"))
		t = append(t, value.NewString("e"))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewInt(8))
		t = append(t, value.NewString("e"))
		t = append(t, value.NewString("g"))
		r.AddTuple(t)
	}
	{
		var t value.Tuple

		t = append(t, value.NewInt(9))
		t = append(t, value.NewString("e"))
		t = append(t, value.NewString("f"))
		r.AddTuple(t)
	}
	return r
}