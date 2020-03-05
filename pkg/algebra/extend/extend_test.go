package extend

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
)

func TestExtend(t *testing.T) {
	testLT()
}

func testLT() {
	a := newTestRelation0()
	{
		fmt.Printf("%s\n", a)
	}
	b := newTestRelation1()
	{
		fmt.Printf("%s\n", b)
	}
	ea, err := NewAttribute("b", a)
	if err != nil {
		log.Fatal(err)
	}
	eb, err := NewAttribute("b", b)
	if err != nil {
		log.Fatal(err)
	}
	e := &BinaryExtend{
		Op:    overload.LT,
		Left:  ea,
		Right: eb,
	}
	as, err := util.GetTuples(a)
	if err != nil {
		log.Fatal(err)
	}
	bs, err := util.GetTuples(b)
	if err != nil {
		log.Fatal(err)
	}
	for _, a := range as {
		for _, b := range bs {
			ok, err := e.Eval([]value.Tuple{a, b})
			if err != nil {
				log.Fatal(err)
			}
			if value.MustBeBool(ok) {
				fmt.Printf("%s < %s\n\t%s\n", a, b, ok)
			}
		}
	}
}

func testTypeof() {
	r := newTestRelation0()
	{
		fmt.Printf("r:\n%s\n", r)
	}
	a, err := NewAttribute("a", r)
	if err != nil {
		log.Fatal(err)
	}
	e := &UnaryExtend{
		E:  a,
		Op: overload.Typeof,
	}
	ts, err := util.GetTuples(r)
	if err != nil {
		log.Fatal(err)
	}
	for i, t := range ts {
		v, err := e.Eval([]value.Tuple{t})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%v: %s.a.type\n\t%s\n", i, t, v)
	}

}

func newTestRelation0() relation.Relation {
	var attrs []string

	attrs = append(attrs, "a")
	attrs = append(attrs, "b")
	attrs = append(attrs, "c")
	r := mem.New("A", attrs)
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
	var attrs []string

	attrs = append(attrs, "b")
	attrs = append(attrs, "d")
	r := mem.New("B", attrs)
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
