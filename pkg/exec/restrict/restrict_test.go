package restrict

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/testunit"
)

func TestRestrict(t *testing.T) {
	ct := context.New()
	r := newTestRelation(ct)
	{
		fmt.Printf("r:\n%s\n", r)
	}
	a := &extend.Attribute{r.Placeholder(), "a"}
	e1 := &extend.UnaryExtend{
		E:  a,
		Op: overload.Typeof,
	}
	e := &extend.BinaryExtend{
		Op:    overload.EQ,
		Left:  e1,
		Right: value.NewString("int"),
	}
	us, err := testunit.NewRestrict(3, e, ct, r)
	if err != nil {
		log.Fatal(err)
	}
	rr, err := New(us, ct).Restrict()
	if err != nil {
		log.Fatal(err)
	}
	{
		fmt.Printf("rr:\n%s\n", rr)
	}
}

func newTestRelation(c context.Context) relation.Relation {
	var attrs []string

	attrs = append(attrs, "a")
	attrs = append(attrs, "b")
	r := mem.New("A", attrs, c)
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
