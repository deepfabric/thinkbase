package mem

import (
	"fmt"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func TestDictionary(t *testing.T) {
	m := New()
	{
		k := value.NewInt(1)
		err := m.IsExit(k)
		fmt.Printf("IsExit: '%v' - %v\n", k, err)
	}
	{
		k := value.NewInt(1)
		err := m.Set(k, "test")
		fmt.Printf("set '%v': %v\n", k, err)
	}
	{
		k := value.NewInt(1)
		err := m.IsExit(k)
		fmt.Printf("IsExit: '%v' - %v\n", k, err)
	}
	{
		k := value.NewInt(1)
		v, err := m.Get(k)
		fmt.Printf("Get: '%s' - %v, %v\n", k, v, err)
	}
	{
		k := value.NewInt(1)
		ok, v, err := m.GetOrSet(k, "t")
		fmt.Printf("GetOrSet: '%v', %v, %v\n", ok, v, err)
	}
	{
		k := value.NewInt(1)
		v, err := m.Get(k)
		fmt.Printf("Get: '%s' - %v, %v\n", k, v, err)
	}
}
