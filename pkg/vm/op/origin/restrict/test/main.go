package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/deepfabric/thinkbase/pkg/storage"
	"github.com/deepfabric/thinkbase/pkg/storage/cache/bitmap/mem"
	rbmem "github.com/deepfabric/thinkbase/pkg/storage/cache/rangebitmap/mem"
	rmem "github.com/deepfabric/thinkbase/pkg/storage/cache/relation/mem"
	srbmem "github.com/deepfabric/thinkbase/pkg/storage/cache/srangebitmap/mem"
	"github.com/deepfabric/thinkbase/pkg/vm/container/relation"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/extend"
	"github.com/deepfabric/thinkbase/pkg/vm/extend/overload"
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
		{
			e := &extend.BinaryExtend{
				Op:    overload.LT,
				Left:  &extend.Attribute{"amount"},
				Right: value.NewInt(1000),
			}

			n := restrict.New(r, e, c)
			{
				fmt.Printf("%s\n", n)
			}
			{
				attrs, err := n.AttributeList()
				fmt.Printf("%v, %v\n", attrs, err)
			}
			bs := c.BlockSize()
			t := time.Now()
			for {
				mp, err := n.GetAttributes([]string{"amount"}, bs)
				if err != nil {
					log.Fatal(err)
				}
				if len(mp) == 0 || len(mp["amount"]) == 0 {
					break
				}
			}
			fmt.Printf("process: %v\n", time.Now().Sub(t))

		}
	}
}

var r *rand.Rand

func init() {
	r = rand.New(rand.NewSource(time.Now().Unix()))
}

func randString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func inject(rp relation.Relation, n int) error {
	var ts []map[string]interface{}

	for i := 0; i < n; i++ {
		if i%100000 == 0 {
			if err := rp.AddTuplesByJson(ts); err != nil {
				return err
			}
			ts = []map[string]interface{}{}
		}
		mp := make(map[string]interface{})
		mp["name"] = randString(5)
		mp["amount"] = r.Int63n(10000)
		mp["count0"] = r.Int63n(10000)
		mp["count1"] = r.Int63n(10000)
		mp["count2"] = r.Int63n(10000)
		mp["count3"] = r.Int63n(10000)
		mp["count4"] = r.Int63n(10000)
		mp["count5"] = r.Int63n(10000)
		mp["count6"] = r.Int63n(10000)
		mp["count7"] = r.Int63n(10000)
		mp["count8"] = r.Int63n(10000)
		mp["count9"] = r.Int63n(10000)
		mp["date"] = time.Unix(r.Int63n(100000000), 0)
		ts = append(ts, mp)
	}
	return rp.AddTuplesByJson(ts)
}
