package relation

import (
	"fmt"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/sql/algebra/value"
)

func TestRelation(t *testing.T) {
	attrs := make([]*AttributeMetadata, 2)
	attrs[0] = &AttributeMetadata{
		Name:  "a",
		Types: make(map[int32]int),
	}
	attrs[1] = &AttributeMetadata{
		Name:  "b",
		Types: make(map[int32]int),
	}
	r := New("A", nil, attrs)
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
		fmt.Printf("r:\n%s\n", r)
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
