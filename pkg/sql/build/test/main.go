package main

import (
	"fmt"
	"log"
	"os"

	"github.com/deepfabric/thinkbase/pkg/sql/build"
	"github.com/deepfabric/thinkbase/pkg/storage"
	"github.com/deepfabric/thinkbase/pkg/storage/cache/bitmap/mem"
	rbmem "github.com/deepfabric/thinkbase/pkg/storage/cache/rangebitmap/mem"
	rmem "github.com/deepfabric/thinkbase/pkg/storage/cache/relation/mem"
	srbmem "github.com/deepfabric/thinkbase/pkg/storage/cache/srangebitmap/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/estimator"
	"github.com/deepfabric/thinkbase/pkg/vm/opt"
	"github.com/deepfabric/thinkbase/pkg/vm/workspace"
	"github.com/deepfabric/thinkkv/pkg/engine/pb"
)

func main() {
	var c context.Context
	{
		db := pb.New("test.db", nil, false)
		stg := storage.New(db, mem.New(), rmem.New(), rbmem.New(), srbmem.New())
		est := estimator.New()
		wsp := workspace.New("tom", "test", stg)
		c = context.New(context.NewConfig("tom"), est, wsp)
	}
	n, err := build.New(os.Args[1], c).Build()
	if err != nil {
		log.Fatal(err)
	}
	{
		fmt.Printf("before optimize: %s\n", n)
	}
	n = opt.New(n, c).Optimize()
	{
		fmt.Printf("after optimize: %s\n", n)
	}
	{
		attrs, err := n.AttributeList()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("attributes = %v\n", attrs)
	}
	bs := c.BlockSize()
	attrs, _ := n.AttributeList()
	for {
		mp, err := n.GetAttributes(attrs, bs)
		if err != nil {
			log.Fatal(err)
		}
		if len(mp) == 0 || len(mp[attrs[0]]) == 0 {
			break
		}
		for i := 0; i < len(mp[attrs[0]]); i++ {
			for j, attr := range attrs {
				if j == 0 {
					fmt.Printf("%s", mp[attr][i])
				} else {
					fmt.Printf("\t%s", mp[attr][i])
				}
			}
			fmt.Printf("\n")
		}
	}
}
