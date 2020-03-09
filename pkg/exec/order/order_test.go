package order

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/testunit"
)

func TestOrder(t *testing.T) {
	ct := context.New()
	r := newTestRelation(ct)
	{
		fmt.Printf("%s\n", r)
	}
	{
		cmp := util.NewCompare(false, []bool{false}, []string{"a"}, r.Metadata())
		us, err := testunit.NewOrder(2, false, []bool{false}, []string{"a"}, ct, r)
		if err != nil {
			log.Fatal(err)
		}
		rr, err := New(us, ct, cmp).Order()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", rr)
	}
	{
		cmp := util.NewCompare(false, []bool{true}, []string{"a"}, r.Metadata())
		us, err := testunit.NewOrder(2, false, []bool{true}, []string{"a"}, ct, r)
		if err != nil {
			log.Fatal(err)
		}
		rr, err := New(us, ct, cmp).Order()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", rr)
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
