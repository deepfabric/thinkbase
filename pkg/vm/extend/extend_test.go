package extend

import (
	"fmt"
	"log"
	"testing"

	"github.com/deepfabric/thinkbase/pkg/vm/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
)

func TestExtend(t *testing.T) {
	testLT()
	testTypeof()
}

func testLT() {
	ea := &Attribute{"r.b"}
	eb := &Attribute{"s.b"}
	e := &BinaryExtend{
		Op:    overload.LT,
		Left:  ea,
		Right: eb,
	}
	mp := make(map[string]value.Value)
	mp["r.b"] = value.NewInt(3)
	mp["s.b"] = value.NewInt(5)
	v, err := e.Eval(mp)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("v: %v\n", v)
}

func testTypeof() {
	a := &Attribute{"a"}
	e := &UnaryExtend{
		E:  a,
		Op: overload.Typeof,
	}
	mp := make(map[string]value.Value)
	mp["a"] = value.NewInt(3)
	v, err := e.Eval(mp)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("v: %v\n", v)
}
