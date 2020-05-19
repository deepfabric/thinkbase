package main

import (
	"fmt"
	"log"
	"time"

	"github.com/deepfabric/thinkbase/pkg/storage"
	"github.com/deepfabric/thinkbase/pkg/storage/cache/bitmap/mem"
	rbmem "github.com/deepfabric/thinkbase/pkg/storage/cache/rangebitmap/mem"
	rmem "github.com/deepfabric/thinkbase/pkg/storage/cache/relation/mem"
	srbmem "github.com/deepfabric/thinkbase/pkg/storage/cache/srangebitmap/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/nub"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/restrict"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
	"github.com/deepfabric/thinkkv/pkg/engine/pb"
)

func main() {
	db := pb.New("test.db", nil, false, false)
	stg := storage.New(db, mem.New(), rmem.New(), rbmem.New(), srbmem.New())
	{
		r, err := stg.Relation("test.A")
		if err != nil {
			log.Fatal(err)
		}
		c := context.New(context.NewConfig("tom"), nil, nil)
		e := &extend.BinaryExtend{
			Op:    overload.GT,
			Left:  &extend.Attribute{"city"},
			Right: value.NewString("H"),
		}
		fp := restrict.New(r, e, c)
		bs := c.BlockSize()
		n := nub.New(fp, []string{"number"}, c)
		{
			fmt.Printf("%s\n", n)
		}
		{
			attrs, err := n.AttributeList()
			fmt.Printf("%v, %v\n", attrs, err)
		}
		t := time.Now()
		fmt.Printf("number\t\tid\t\tsex\tprice\n")
		for {
			mp, err := n.GetAttributes([]string{"number", "id", "sex", "price"}, bs)
			if err != nil {
				log.Fatal(err)
			}
			if len(mp) == 0 || len(mp["number"]) == 0 {
				break
			}
			for i := 0; i < len(mp["number"]); i++ {
				fmt.Printf("%s\t\t%s\t%s\t%s\n", mp["number"][i], mp["id"][i], mp["sex"][i], mp["price"][i])
			}
		}
		fmt.Printf("process: %v\n", time.Now().Sub(t))
	}
}
