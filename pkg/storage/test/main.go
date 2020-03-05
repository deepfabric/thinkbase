package main

import (
	"fmt"
	"log"
	"time"

	"github.com/deepfabric/thinkbase/pkg/storage"
	"github.com/deepfabric/thinkbase/pkg/storage/engine/bg"
)

func main() {
	db, err := storage.New(bg.New("test.db"))
	if err != nil {
		log.Fatal(err)
	}
	tbl, err := db.Table("test")
	if err != nil {
		log.Fatal(err)
	}
	{
		mp := make(map[string]interface{})
		mp["a"] = int64(1)
		mp["b"] = float64(3.3)
		mp["c"] = string("fxxf")
		{
			var xs []interface{}

			xs = append(xs, int64(2))
			xs = append(xs, time.Now())
			{
				mq := make(map[string]interface{})
				mq["aa"] = float64(1.0)
				xs = append(xs, mq)
			}
			mp["d"] = xs
		}
		{
			{
				mq := make(map[string]interface{})
				mq["cc"] = string("fgag")
				mq["test"] = int64(354)
				mp["e"] = mq
			}
		}
		if err := tbl.AddTuple(mp); err != nil {
			log.Fatal(err)
		}
	}
	{
		mp := make(map[string]interface{})
		mp["a"] = bool(true)
		mp["b"] = float64(4324)
		mp["c"] = string("fagadfg")
		{
			var xs []interface{}

			xs = append(xs, time.Now())
			xs = append(xs, int64(3))
			xs = append(xs, bool(true))
			mp["d"] = xs
		}
		{
			{
				mq := make(map[string]interface{})
				mq["cc"] = string("fgag")
				mq["test"] = int64(354)
				mp["f"] = mq
			}
		}
		if err := tbl.AddTuple(mp); err != nil {
			log.Fatal(err)
		}
	}
	{
		cnt, err := tbl.GetTupleCount()
		if err != nil {
			log.Fatal(err)
		}
		attrs := tbl.Metadata()
		ts, err := tbl.GetTuples(0, cnt)
		if err != nil {
			log.Fatal(err)
		}
		for i, j := 0, len(attrs); i < j; i++ {
			if i > 0 {
				fmt.Printf(", ")
			}
			fmt.Printf("%s", attrs[i])
		}
		fmt.Printf("\n")
		for _, t := range ts {
			fmt.Printf("%s\n", t)
		}
	}

}
