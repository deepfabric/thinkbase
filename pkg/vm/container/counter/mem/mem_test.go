package mem

import (
	"fmt"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func TestCounter(t *testing.T) {
	m := New()
	{
		err := m.Inc(value.NewInt(1))
		fmt.Printf("%v\n", err)
	}
	{
		err := m.Inc(value.NewInt(1))
		fmt.Printf("%v\n", err)
	}
	{
		ok, err := m.Dec(value.NewInt(1))
		fmt.Printf("%v, %v\n", ok, err)
	}
	{
		ok, err := m.Dec(value.NewInt(1))
		fmt.Printf("%v, %v\n", ok, err)
	}
	{
		ok, err := m.Dec(value.NewInt(1))
		fmt.Printf("%v, %v\n", ok, err)
	}
}
