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
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/group"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/restrict"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize"
	aoverload "github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload"
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
		{
			var es []*summarize.Extend

			es = append(es, &summarize.Extend{
				Name:  "amount",
				Alias: "A",
				Op:    aoverload.Avg,
			})
			es = append(es, &summarize.Extend{
				Name:  "date",
				Alias: "B",
				Op:    aoverload.Max,
			})
			es = append(es, &summarize.Extend{
				Name:  "price",
				Alias: "C",
				Op:    aoverload.Sum,
			})
			es = append(es, &summarize.Extend{
				Name:  "date",
				Alias: "D",
				Op:    aoverload.Min,
			})
			es = append(es, &summarize.Extend{
				Name:  "name",
				Alias: "E",
				Op:    aoverload.Min,
			})
			es = append(es, &summarize.Extend{
				Name:  "price",
				Alias: "F",
				Op:    aoverload.Max,
			})
			es = append(es, &summarize.Extend{
				Name:  "price",
				Alias: "G",
				Op:    aoverload.Count,
			})
			es = append(es, &summarize.Extend{
				Name:  "id",
				Alias: "H",
				Op:    aoverload.Count,
			})
			es = append(es, &summarize.Extend{
				Name:  "amount",
				Alias: "I",
				Op:    aoverload.Count,
			})
			es = append(es, &summarize.Extend{
				Name:  "vip",
				Alias: "L",
				Op:    aoverload.Count,
			})
			n := group.New(fp, nil, []string{"number"}, es, c)
			{
				fmt.Printf("%s\n", n)
			}
			{
				attrs, err := n.AttributeList()
				fmt.Printf("%v, %v\n", attrs, err)
			}
			bs := c.BlockSize()
			t := time.Now()
			fmt.Printf("number\tA\n")
			for {
				mp, err := n.GetAttributes([]string{"number", "A"}, bs)
				if err != nil {
					log.Fatal(err)
				}
				if len(mp) == 0 || len(mp["A"]) == 0 {
					break
				}
				for i := 0; i < len(mp["A"]); i++ {
					fmt.Printf("%s\t%s\n", mp["number"][i], mp["A"][i])
				}
			}
			fmt.Printf("process: %v\n", time.Now().Sub(t))
		}
	}
}
