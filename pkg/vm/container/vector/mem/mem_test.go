package mem

import (
	"fmt"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func TestVector(t *testing.T) {
	m := New()
	{
		ok, err := m.IsEmpty()
		fmt.Printf("%v, %v\n", ok, err)
	}
	{
		var a value.Array

		a = append(a, value.NewInt(1))
		a = append(a, value.NewString("x"))
		m.Append(a)
	}
	{
		var a value.Array

		a = append(a, value.NewInt(3))
		a = append(a, value.NewString("y"))
		m.Append(a)
	}
	{
		ok, err := m.IsEmpty()
		fmt.Printf("%v, %v\n", ok, err)
	}
	{
		v, err := m.Pop()
		fmt.Printf("%v, %v\n", v, err)
	}
	{
		v, err := m.Pop()
		fmt.Printf("%v, %v\n", v, err)
	}
}
