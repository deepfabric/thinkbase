package restrict

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func TestRestrict(t *testing.T) {
	{
		r := newRelation()
		fmt.Printf("%s\n", r.DataString())
	}
	r := newRelation()
	e := &extend.BinaryExtend{
		Op:    overload.GT,
		Left:  &extend.Attribute{"a"},
		Right: value.NewInt(1),
	}
	c := context.New(context.NewConfig("tom"), nil, nil)
	n := New(r, e, c)
	{
		fmt.Printf("%s\n", n)
	}
	{
		attrs, err := n.AttributeList()
		fmt.Printf("%v, %v\n", attrs, err)
	}
	bs := n.c.BlockSize()
	for {
		mp, err := n.GetAttributes([]string{"a", "b"}, bs)
		if err != nil {
			log.Fatal(err)
		}
		if len(mp["a"]) == 0 {
			break
		}
		fmt.Printf("a = %v\n", mp["a"])
		fmt.Printf("b = %v\n", mp["b"])
	}
}

func newRelation() relation.Relation {
	var attrs []string

	attrs = append(attrs, "a")
	attrs = append(attrs, "b")
	r := mem.New("A", attrs)
	{
		var t value.Array

		t = append(t, value.NewInt(1))
		t = append(t, value.NewString("x"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewInt(3))
		t = append(t, value.NewString("y"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewInt(2))
		t = append(t, value.NewString("m"))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewFloat(3.1))
		t = append(t, value.NewInt(3))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewFloat(3.1))
		t = append(t, value.NewInt(3))
		r.AddTuples([]value.Array{t})
	}
	{
		var t value.Array

		t = append(t, value.NewInt(1))
		t = append(t, value.NewString("x"))
		r.AddTuples([]value.Array{t})
	}
	return r
}
