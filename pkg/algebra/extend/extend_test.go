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
	"github.com/deepfabric/thinkbase/pkg/context"
)

func TestExtend(t *testing.T) {
	testLT()
	//	testTypeof()
}

func testLT() {
	ct := context.New()
	a := newTestRelation0(ct)
	{
		fmt.Printf("%s\n", a)
	}
	b := newTestRelation1(ct)
	{
		fmt.Printf("%s\n", b)
	}
	ea := &Attribute{a.Placeholder(), "b"}
	eb := &Attribute{b.Placeholder(), "b"}
	e := &BinaryExtend{
		Op:    overload.LT,
		Left:  ea,
		Right: eb,
	}
	acnt, err := a.GetTupleCount()
	if err != nil {
		log.Fatal(err)
	}
	bcnt, err := b.GetTupleCount()
	if err != nil {
		log.Fatal(err)
	}
	mp, as, bs, err := util.GetattributeByJoin(a.Placeholder(), b.Placeholder(), e.Attributes(), ct)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < acnt; i++ {
		for j := 0; j < bcnt; j++ {
			var at, bt value.Tuple

			for _, attrs := range as {
				at = append(at, attrs[i])
			}
			for _, attrs := range bs {
				bt = append(bt, attrs[j])
			}
			ok, err := e.Eval([]value.Tuple{at, bt}, mp)
			if err != nil {
				log.Fatal(err)
			}
			if value.MustBeBool(ok) {
				fmt.Printf("a.%v < b.%v\n\t%s\n", i, j, ok)
			}
		}
	}
}

func testTypeof() {
	ct := context.New()
	r := newTestRelation0(ct)
	{
		fmt.Printf("r:\n%s\n", r)
	}
	a := &Attribute{r.Placeholder(), "a"}
	e := &UnaryExtend{
		E:  a,
		Op: overload.Typeof,
	}
	cnt, err := r.GetTupleCount()
	if err != nil {
		log.Fatal(err)
	}
	mp, as, err := util.Getattribute(r.Placeholder(), e.Attributes(), ct)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < cnt; i++ {
		var t value.Tuple

		for _, attrs := range as {
			t = append(t, attrs[i])
		}
		v, err := e.Eval([]value.Tuple{t, t}, mp)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%v: a.type\n\t%s\n", i, v)
	}
}

func newTestRelation0(ct context.Context) relation.Relation {
	var attrs []string

	attrs = append(attrs, "a")
	attrs = append(attrs, "b")
	attrs = append(attrs, "c")
	r := mem.New("A", attrs, ct)
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

func newTestRelation1(ct context.Context) relation.Relation {
	var attrs []string

	attrs = append(attrs, "b")
	attrs = append(attrs, "d")
	r := mem.New("B", attrs, ct)
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
