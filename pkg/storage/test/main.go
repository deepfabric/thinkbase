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
	"github.com/deepfabric/thinkbase/pkg/vm/value"
	"github.com/deepfabric/thinkkv/pkg/engine/pb"
)

func main() {
	db := pb.New("test.db", nil, 0, false, false)
	stg := storage.New(db, mem.New(), rmem.New(), rbmem.New(), srbmem.New())
	{
		r, err := stg.Relation("tom.test.A")
		if err != nil {
			log.Fatal(err)
		}
		{
			t := time.Now()
			/*
				if err := inject(r, 10000000); err != nil {
					log.Fatal(err)
				}
			*/
			if err := inject(r, 30); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("inject: %v\n", time.Now().Sub(t))
		}
	}
	/*
		{
			r, err := stg.Relation("test.A")
			if err != nil {
				log.Fatal(err)
			}
			if err := load(r); err != nil {
				log.Fatal(err)
			}
			for {
				mp, err := r.GetAttributes([]string{"a", "b", "c"}, 10000)
				if err != nil {
					log.Fatal(err)
				}
				if len(mp) == 0 || len(mp["a"]) == 0 {
					break
				}
				fmt.Printf("\ta = %v\n", mp["a"])
				fmt.Printf("\tb = %v\n", mp["b"])
				fmt.Printf("\tc = %v\n", mp["c"])
			}
			imp, err := r.Eq("a", value.NewString("x"), 0)
			if err != nil {
				log.Fatal(err)
			}
			is := imp.Slice()
			fmt.Printf("a == 'x': %v\n", is)
			for {
				mp, err := r.GetAttributesByIndex([]string{"a", "b", "c"}, is, 10000)
				if err != nil {
					log.Fatal(err)
				}
				if len(mp) == 0 || len(mp["a"]) == 0 {
					break
				}
				is = is[len(mp["a"]):]
				fmt.Printf("\ta = %v\n", mp["a"])
				fmt.Printf("\tb = %v\n", mp["b"])
				fmt.Printf("\tc = %v\n", mp["c"])
			}
			imp, err = r.Ne("a", value.NewString("x"), 0)
			if err != nil {
				log.Fatal(err)
			}
			is = imp.Slice()
			fmt.Printf("a <> 'x': %v\n", is)
			for {
				mp, err := r.GetAttributesByIndex([]string{"a", "b", "c"}, is, 10000)
				if err != nil {
					log.Fatal(err)
				}
				if len(mp) == 0 || len(mp["a"]) == 0 {
					break
				}
				is = is[len(mp["a"]):]
				fmt.Printf("\ta = %v\n", mp["a"])
				fmt.Printf("\tb = %v\n", mp["b"])
				fmt.Printf("\tc = %v\n", mp["c"])
			}
			imp, err = r.Ne("b", value.NewInt(3), 0)
			if err != nil {
				log.Fatal(err)
			}
			is = imp.Slice()
			fmt.Printf("b <> 3: %v\n", is)
			for {
				mp, err := r.GetAttributesByIndex([]string{"a", "b", "c"}, is, 10000)
				if err != nil {
					log.Fatal(err)
				}
				if len(mp) == 0 || len(mp["a"]) == 0 {
					break
				}
				is = is[len(mp["a"]):]
				fmt.Printf("\ta = %v\n", mp["a"])
				fmt.Printf("\tb = %v\n", mp["b"])
				fmt.Printf("\tc = %v\n", mp["c"])
			}
			imp, err = r.Ne("c", value.NewBool(true), 0)
			if err != nil {
				log.Fatal(err)
			}
			is = imp.Slice()
			fmt.Printf("c <> true: %v\n", is)
			for {
				mp, err := r.GetAttributesByIndex([]string{"a", "b", "c"}, is, 10000)
				if err != nil {
					log.Fatal(err)
				}
				if len(mp) == 0 || len(mp["a"]) == 0 {
					break
				}
				is = is[len(mp["a"]):]
				fmt.Printf("\ta = %v\n", mp["a"])
				fmt.Printf("\tb = %v\n", mp["b"])
				fmt.Printf("\tc = %v\n", mp["c"])
			}
			imp, err = r.Lt("b", value.NewInt(4), 0)
			if err != nil {
				log.Fatal(err)
			}
			is = imp.Slice()
			fmt.Printf("b < 4: %v\n", is)
			for {
				mp, err := r.GetAttributesByIndex([]string{"a", "b", "c"}, is, 10000)
				if err != nil {
					log.Fatal(err)
				}
				if len(mp) == 0 || len(mp["a"]) == 0 {
					break
				}
				is = is[len(mp["a"]):]
				fmt.Printf("\ta = %v\n", mp["a"])
				fmt.Printf("\tb = %v\n", mp["b"])
				fmt.Printf("\tc = %v\n", mp["c"])
			}
		}
	*/
	/*
		{
			r, err := stg.Relation("test.A.c")
			if err != nil {
				log.Fatal(err)
			}
			for {
				mp, err := r.GetAttributes([]string{"d", "f"}, 1024*1024)
				if err != nil {
					log.Fatal(err)
				}
				if len(mp) == 0 || len(mp["d"]) == 0 {
					break
				}
				fmt.Printf("d = %v\n", mp["d"])
				fmt.Printf("f = %v\n", mp["f"])
			}
		}
	*/
	/*
		{
			r, err := stg.Relation("test.A.a_0")
			if err != nil {
				log.Fatal(err)
			}
			for {
				mp, err := r.GetAttributes([]string{"_", "d", "f"}, 1024*1024)
				if err != nil {
					log.Fatal(err)
				}
				if len(mp) == 0 || len(mp["_"]) == 0 {
					break
				}
				fmt.Printf("_ = %v\n", mp["_"])
				fmt.Printf("d = %v\n", mp["d"])
				fmt.Printf("f = %v\n", mp["f"])
			}
		}
	*/
	/*
		{
			r, err := stg.Relation("test.A.a_1")
			if err != nil {
				log.Fatal(err)
			}
			{
				attrs, _ := r.AttributeList()
				fmt.Printf("%v\n", attrs)
			}
			for {
				mp, err := r.GetAttributes([]string{"_"}, 1024*1024)
				if err != nil {
					log.Fatal(err)
				}
				if len(mp) == 0 || len(mp["_"]) == 0 {
					break
				}
				fmt.Printf("_ = %v\n", mp["_"])
			}
		}
	*/
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

func load(rp relation.Relation) error {
	var ts []map[string]interface{}

	{
		mp := make(map[string]interface{})
		mp["a"] = "x"
		mp["c"] = true
		ts = append(ts, mp)
	}
	{
		mp := make(map[string]interface{})
		mp["a"] = "a"
		mp["b"] = int64(3)
		mp["c"] = "y"
		ts = append(ts, mp)
	}
	{
		mp := make(map[string]interface{})
		mp["a"] = "b"
		mp["c"] = "m"
		ts = append(ts, mp)
	}
	{
		mp := make(map[string]interface{})
		mp["a"] = "c"
		mp["b"] = float64(3.1)
		{
			mq := make(map[string]interface{})
			mq["d"] = "hello"
			mq["f"] = float64(11.11)
			mp["c"] = mq
		}
		ts = append(ts, mp)
	}
	{
		mp := make(map[string]interface{})
		{
			var xs []interface{}

			xs = append(xs, int64(3))
			xs = append(xs, int64(2))
			mp["a"] = xs
		}
		mp["b"] = "hello world"
		ts = append(ts, mp)
	}
	{
		mp := make(map[string]interface{})
		{
			var xs []interface{}

			mq := make(map[string]interface{})
			mq["d"] = int64(13)
			t, _ := value.ParseTime("2020-04-18 12:35:40")
			mq["f"] = value.MustBeTime(t)
			xs = append(xs, mq)
			mp["a"] = xs
		}
		ts = append(ts, mp)
	}
	{
		mp := make(map[string]interface{})
		{
			mq := make(map[string]interface{})
			mq["f"] = float64(12.22)
			mp["c"] = mq
		}
		ts = append(ts, mp)
	}
	{
		mp := make(map[string]interface{})
		mp["c"] = false
		ts = append(ts, mp)
	}
	return rp.AddTuplesByJson(ts)
}

func inject(rp relation.Relation, n int) error {
	var ts []map[string]interface{}

	for i := 0; i < n; i++ {
		if i%200000 == 0 {
			if err := rp.AddTuplesByJson(ts); err != nil {
				return err
			}
			ts = []map[string]interface{}{}
		}
		mp := make(map[string]interface{})
		mp["name"] = randString(int(rand.Int63n(100)))
		mp["id"] = randString(10)
		mp["sex"] = randString(2)
		mp["number"] = randString(1)
		mp["city"] = randString(8)
		mp["phone"] = randString(15)
		mp["amount"] = r.Int63n(10000000)
		mp["price"] = float64(rand.Int63n(10000))
		if i%2 == 0 {
			mp["vip"] = true
		} else {
			mp["vip"] = false
		}
		mp["date"] = time.Unix(r.Int63n(100000000000), 0)
		ts = append(ts, mp)
	}
	return rp.AddTuplesByJson(ts)
}
