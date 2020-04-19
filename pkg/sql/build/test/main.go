package main

import (
	"fmt"
	"log"
	"os"

	"github.com/deepfabric/thinkbase/pkg/sql/build"
	"github.com/deepfabric/thinkbase/pkg/vm/context/testContext"
	"github.com/deepfabric/thinkbase/pkg/vm/opt"
)

func main() {
	n, err := build.New(os.Args[1], testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024)).Build()
	if err != nil {
		log.Fatal(err)
	}
	n = opt.New(n).Optimize()
	{
		fmt.Printf("%s\n", n)
	}
	{
		attrs, err := n.AttributeList()
		fmt.Printf("%v, %v\n", attrs, err)
	}
	for {
		ts, err := n.GetTuples(1024 * 1024)
		if err != nil {
			log.Fatal(err)
		}
		if len(ts) == 0 {
			break
		}
		for i, t := range ts {
			fmt.Printf("[%v] = %v\n", i, t)
		}
	}
}
