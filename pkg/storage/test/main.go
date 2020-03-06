package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/extend/overload"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/disk"
	asummarize "github.com/deepfabric/thinkbase/pkg/algebra/summarize"
	aoverload "github.com/deepfabric/thinkbase/pkg/algebra/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/exec/restrict"
	"github.com/deepfabric/thinkbase/pkg/exec/summarize"
	"github.com/deepfabric/thinkbase/pkg/exec/testunit"
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
	if err := inject(tbl, 100); err != nil {
		log.Fatal(err)
	}
	{
		fmt.Printf("++++++inject end++++++\n")
	}
	testSummarize("test", db)
}

func testRestrict(id string, db storage.Database) {
	r, err := disk.New(id, db)
	if err != nil {
		log.Fatal(err)
	}
	a, err := extend.NewAttribute("amount", r)
	if err != nil {
		log.Fatal(err)
	}
	e := &extend.BinaryExtend{
		Op:    overload.GT,
		Left:  a,
		Right: value.NewInt(1000),
	}
	t := time.Now()
	us, err := testunit.NewRestrict(2, e, r)
	if err != nil {
		log.Fatal(err)
	}
	_, err = restrict.New(us).Restrict()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("process: %v\n", time.Now().Sub(t))
}

func testSummarize(id string, db storage.Database) {
	r, err := disk.New(id, db)
	if err != nil {
		log.Fatal(err)
	}
	ops := []int{}
	gs := []string{}
	attrs := []*asummarize.Attribute{}
	{
		ops = append(ops, aoverload.Avg)
		attrs = append(attrs, &asummarize.Attribute{Name: "amount", Alias: "A"})
	}
	t := time.Now()
	us, err := testunit.NewSummarize(2, ops, gs, attrs, r)
	if err != nil {
		log.Fatal(err)
	}
	_, err = summarize.New(ops, gs, attrs, r, us).Summarize()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("process: %v\n", time.Now().Sub(t))
}

var r *rand.Rand

func init() {
	r = rand.New(rand.NewSource(time.Now().Unix()))
}

func inject(tbl storage.Table, n int) error {
	for i := 0; i < n; i++ {
		mp := make(map[string]interface{})
		mp["name"] = randString(5)
		mp["amount"] = r.Int63n(10000)
		/*
			mp["count0"] = r.Int63n(10000)
			mp["count1"] = r.Int63n(10000)
			mp["count2"] = r.Int63n(10000)
		*/
		mp["date"] = time.Unix(r.Int63n(100000000), 0)
		if err := tbl.AddTuple(mp); err != nil {
			return err
		}
	}
	return nil
}

func randString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := r.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}
