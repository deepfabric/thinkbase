package main

import (
	"fmt"
	"log"
	"time"

	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/util/encoding"
)

func main() {
	var v value.Array

	v = append(v, value.NewInt(123))
	v = append(v, value.NewFloat(124.545))
	v = append(v, value.NewBool(true))
	v = append(v, value.NewString("fabg"))
	v = append(v, value.ConstNull)
	{
		var v0 value.Array

		v0 = append(v0, value.NewBool(false))
		v0 = append(v0, value.NewString("xxx"))
		v = append(v, v0)
	}
	v = append(v, value.NewTime(time.Now()))
	v = append(v, value.NewTable("test"))

	data, err := encoding.Encode(v)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%v\n", v)
	fmt.Printf("%v\n", len(data))
	a, _, err := encoding.Decode(data)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%v\n", a)
}
