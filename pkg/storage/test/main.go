package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/extend/overload"
	aprojection "github.com/deepfabric/thinkbase/pkg/algebra/projection"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/disk"
	asummarize "github.com/deepfabric/thinkbase/pkg/algebra/summarize"
	aoverload "github.com/deepfabric/thinkbase/pkg/algebra/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/order"
	"github.com/deepfabric/thinkbase/pkg/exec/projection"
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
	/*
		tbl, err := db.Table("test")
		if err != nil {
			log.Fatal(err)
		}
		if err := inject(tbl, 1000000); err != nil {
			log.Fatal(err)
		}
		{
			fmt.Printf("++++++inject end++++++\n")
		}
	*/
	testOrder("test", db)
	//testSummarize("test", db)
	//testRestrict("test", db)
	//testProjection("test", db)
}

func testOrder(id string, db storage.Database) {
	ct := context.New()
	r, err := disk.New(id, db, ct)
	if err != nil {
		log.Fatal(err)
	}
	{
		cmp := util.NewCompare(false, []bool{false}, []string{"name"}, r.Metadata())
		t := time.Now()
		us, err := testunit.NewOrder(4, false, []bool{false}, []string{"a"}, ct, r)
		if err != nil {
			log.Fatal(err)
		}
		_, err = order.New(us, ct, cmp).Order()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("process: %v\n", time.Now().Sub(t))
	}
}

func testRestrict(id string, db storage.Database) {
	ct := context.New()
	r, err := disk.New(id, db, ct)
	if err != nil {
		log.Fatal(err)
	}
	a := &extend.Attribute{r.Placeholder(), "amount"}
	e := &extend.BinaryExtend{
		Op:    overload.GT,
		Left:  a,
		Right: value.NewInt(5000),
	}
	t := time.Now()
	us, err := testunit.NewRestrict(8, e, ct, r)
	if err != nil {
		log.Fatal(err)
	}
	_, err = restrict.New(us, ct).Restrict()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("process: %v\n", time.Now().Sub(t))
}

func testSummarize(id string, db storage.Database) {
	ct := context.New()
	r, err := disk.New(id, db, ct)
	if err != nil {
		log.Fatal(err)
	}
	ops := []int{}
	gs := []string{}
	attrs := []*asummarize.Attribute{}
	{
		gs = append(gs, "name")
	}
	{
		ops = append(ops, aoverload.Sum)
		attrs = append(attrs, &asummarize.Attribute{Name: "amount", Alias: "A"})
	}
	t := time.Now()
	us, err := testunit.NewSummarize(4, ops, gs, attrs, ct, r)
	if err != nil {
		log.Fatal(err)
	}
	_, err = summarize.New(ops, gs, attrs, ct, r, us).Summarize()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("process: %v\n", time.Now().Sub(t))
}

func testProjection(id string, db storage.Database) {
	ct := context.New()
	r, err := disk.New(id, db, ct)
	if err != nil {
		log.Fatal(err)
	}
	attrs := []*aprojection.Attribute{}
	{
		a := &extend.Attribute{r.Placeholder(), "amount"}
		e := &extend.BinaryExtend{
			Op:    overload.Mult,
			Left:  a,
			Right: value.NewInt(5),
		}
		attrs = append(attrs, &aprojection.Attribute{Alias: "A", E: e})
	}
	{
		a := &extend.Attribute{r.Placeholder(), "name"}
		attrs = append(attrs, &aprojection.Attribute{Alias: "B", E: a})
	}
	t := time.Now()
	us, err := testunit.NewProjection(4, attrs, ct, r)
	if err != nil {
		log.Fatal(err)
	}
	_, err = projection.New(us, ct).Projection()
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
