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
	"github.com/deepfabric/thinkbase/pkg/vm/op/index/filter"
	"github.com/deepfabric/thinkbase/pkg/vm/op/index/summarize"
	"github.com/deepfabric/thinkbase/pkg/vm/op/origin/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/vm/types"
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
		var cs []*filter.Condition
		cs = append(cs, &filter.Condition{
			IsOr: true,
			Name: "city",
			Op:   filter.GT,
			Val:  value.NewString("H"),
		})
		{
			var es []*summarize.Extend

			es = append(es, &summarize.Extend{
				Name:  "amount",
				Alias: "A",
				Typ:   types.T_int,
				Op:    overload.Avg,
			})
			es = append(es, &summarize.Extend{
				Name:  "date",
				Alias: "B",
				Typ:   types.T_time,
				Op:    overload.Max,
			})
			es = append(es, &summarize.Extend{
				Name:  "price",
				Alias: "C",
				Typ:   types.T_float,
				Op:    overload.Sum,
			})
			es = append(es, &summarize.Extend{
				Name:  "date",
				Alias: "D",
				Typ:   types.T_time,
				Op:    overload.Min,
			})
			es = append(es, &summarize.Extend{
				Name:  "name",
				Alias: "E",
				Typ:   types.T_string,
				Op:    overload.Min,
			})
			es = append(es, &summarize.Extend{
				Name:  "price",
				Alias: "F",
				Typ:   types.T_float,
				Op:    overload.Max,
			})
			es = append(es, &summarize.Extend{
				Name:  "price",
				Alias: "G",
				Typ:   types.T_float,
				Op:    overload.Count,
			})
			es = append(es, &summarize.Extend{
				Name:  "id",
				Alias: "H",
				Typ:   types.T_string,
				Op:    overload.Count,
			})
			es = append(es, &summarize.Extend{
				Name:  "amount",
				Alias: "I",
				Typ:   types.T_int,
				Op:    overload.Count,
			})
			es = append(es, &summarize.Extend{
				Name:  "vip",
				Alias: "L",
				Typ:   types.T_bool,
				Op:    overload.Count,
			})
			c := context.New(context.NewConfig("tom"), nil, nil)
			n := summarize.New(r, filter.New(cs), es, c)
			{
				fmt.Printf("%s\n", n)
			}
			{
				attrs, err := n.AttributeList()
				fmt.Printf("%v, %v\n", attrs, err)
			}
			t := time.Now()
			bs := c.BlockSize()
			for {
				mp, err := n.GetAttributes([]string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "L"}, bs)
				if err != nil {
					log.Fatal(err)
				}
				if len(mp) == 0 || len(mp["A"]) == 0 {
					break
				}
				fmt.Printf("mp: %v\n", mp)
			}
			fmt.Printf("process: %v\n", time.Now().Sub(t))
		}
	}
}
