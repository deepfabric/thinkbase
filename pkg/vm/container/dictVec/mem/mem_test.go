package mem

import (
	"fmt"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func TestDictvec(t *testing.T) {
	m := New()
	{
		v, err := m.Pop("test")
		fmt.Printf("pop: %v, %v\n", v, err)
	}
	{
		err := m.Push("test", value.Array{value.NewInt(1)})
		fmt.Printf("push: %v\n", err)
	}
	{
		v, err := m.Pop("test")
		fmt.Printf("pop: %v, %v\n", v, err)
	}
}
