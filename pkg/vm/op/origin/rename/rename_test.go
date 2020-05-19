package rename

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/op"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/restrict"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func TestRename(t *testing.T) {
	{
		r := newRelation()
		fmt.Printf("%s\n", r.DataString())
	}
	c := context.New(context.NewConfig("tom"), nil, nil)
	{
		n := newRename(c)
		{
			fmt.Printf("%s\n", n)
		}
		{
			name, err := n.Name()
			fmt.Printf("%v, %v\n", name, err)
		}
		{
			attrs, err := n.AttributeList()
			fmt.Printf("%v, %v\n", attrs, err)
		}
		bs := c.BlockSize()
		for {
			mp, err := n.GetAttributes([]string{"x", "b"}, bs)
			if err != nil {
				log.Fatal(err)
			}
			if len(mp["x"]) == 0 {
				break
			}
			fmt.Printf("x = %v\n", mp["x"])
			fmt.Printf("b = %v\n", mp["b"])
		}
	}
}

func newRestrict(c context.Context) op.OP {
	e := &extend.BinaryExtend{
		Op:    overload.GT,
		Left:  &extend.Attribute{"x"},
		Right: value.NewInt(1),
	}
	return restrict.New(newRestrict(c), e, c)
}

func newRename(c context.Context) op.OP {
	mp := make(map[string]string)
	mp["a"] = "x"
	return New(newRelation(), "C", mp, c)
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
