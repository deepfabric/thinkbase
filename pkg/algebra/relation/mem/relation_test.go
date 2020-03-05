package mem

import (
	"fmt"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/algebra/value"
)

func TestRelation(t *testing.T) {
	var attrs []string

	attrs = append(attrs, "a")
	attrs = append(attrs, "b")
	r := New("A", attrs)
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

	{
		fmt.Printf("%s\n", r)
	}

	r.Nub()

	{
		fmt.Printf("%s after nub:\n%s\n", r.Name(), r)
	}

	r.Sort([]string{"a"}, []bool{false})
	{
		fmt.Printf("%s after sort:\n%s\n", r.Name(), r)
	}
	r.Sort([]string{"a"}, []bool{true})
	{
		fmt.Printf("%s after desc sort:\n%s\n", r.Name(), r)
	}
}
