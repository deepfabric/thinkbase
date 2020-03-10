package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/extend/overload"
	aorder "github.com/deepfabric/thinkbase/pkg/algebra/order"
	aprojection "github.com/deepfabric/thinkbase/pkg/algebra/projection"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/disk"
	asummarize "github.com/deepfabric/thinkbase/pkg/algebra/summarize"
	aoverload "github.com/deepfabric/thinkbase/pkg/algebra/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/minus"
	"github.com/deepfabric/thinkbase/pkg/exec/nub"
	"github.com/deepfabric/thinkbase/pkg/exec/order"
	"github.com/deepfabric/thinkbase/pkg/exec/projection"
	"github.com/deepfabric/thinkbase/pkg/exec/restrict"
	"github.com/deepfabric/thinkbase/pkg/exec/summarize"
	"github.com/deepfabric/thinkbase/pkg/exec/testunit"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
	"github.com/deepfabric/thinkbase/pkg/storage"
	"github.com/deepfabric/thinkbase/pkg/storage/engine/bg"
)

func main() {
	db, err := storage.New(bg.New("test.db"))
	if err != nil {
		log.Fatal(err)
	}
	/*
		tbl0, err := db.Table("test0")
		if err != nil {
			log.Fatal(err)
		}
		tbl1, err := db.Table("test1")
		if err != nil {
			log.Fatal(err)
		}
		{
			if err := inject(tbl0, 1000000); err != nil {
				log.Fatal(err)
			}
			if err := inject(tbl1, 1000000); err != nil {
				log.Fatal(err)
			}
		}
	*/
	{
		fmt.Printf("++++++inject end++++++\n")
	}
	testMinus("test0", "test1", db)
	/*
		testMinus("test0", "test1", db)
		testNub("test0", db)
		testOrder("test0", db)
		testSummarize("test0", db)
		testRestrict("test0", db)
		testProjection("test0", db)
	*/

	/*
		db0, err := storage.New(bg.New("test0.db"))
		if err != nil {
			log.Fatal(err)
		}
		db1, err := storage.New(bg.New("test1.db"))
		if err != nil {
			log.Fatal(err)
		}
			tbl0, err := db0.Table("test0")
			if err != nil {
				log.Fatal(err)
			}
			tbl1, err := db1.Table("test0")
			if err != nil {
				log.Fatal(err)
			}
			{
				if err := inject(tbl0, 1000000); err != nil {
					log.Fatal(err)
				}
				if err := inject(tbl1, 1000000); err != nil {
					log.Fatal(err)
				}
			}
			{
				fmt.Printf("++++++inject end++++++\n")
			}
			testNub("test0", db0)
			testOrder("test0", db0)
			testSummarize("test0", db0)
			testRestrict("test0", db0)
			testProjection("test0", db0)
			testMinus("test0", db0, db1)
	*/
}

func testMinus(id0, id1 string, db storage.Database) {
	var err error
	var r0, r1 relation.Relation

	ct := context.New()
	r0, err = disk.New(id0, db, ct)
	if err != nil {
		log.Fatal(err)
	}
	r1, err = disk.New(id1, db, ct)
	if err != nil {
		log.Fatal(err)
	}
	t := time.Now()
	{
		us, err := testunit.NewNub(4, ct, r0)
		if err != nil {
			log.Fatal(err)
		}
		r0, err = nub.New(us, ct).Nub()
		if err != nil {
			log.Fatal(err)
		}
	}
	{
		us, err := testunit.NewNub(4, ct, r1)
		if err != nil {
			log.Fatal(err)
		}
		r1, err = nub.New(us, ct).Nub()
		if err != nil {
			log.Fatal(err)
		}
	}
	us, err := testunit.New(4, unit.Minus, ct, r0, r1)
	if err != nil {
		log.Fatal(err)
	}
	_, err = minus.New(us, ct).Minus()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("minus process: %v\n", time.Now().Sub(t))
}

/*
func testMinus(id string, db0, db1 storage.Database) {
	var err error
	var r0, r1 relation.Relation

	ct := context.New()
	r0, err = disk.New(id, db0, ct)
	if err != nil {
		log.Fatal(err)
	}
	r1, err = disk.New(id, db1, ct)
	if err != nil {
		log.Fatal(err)
	}
	t := time.Now()
	{
		us, err := testunit.NewNub(4, ct, r0)
		if err != nil {
			log.Fatal(err)
		}
		r0, err = nub.New(us, ct).Nub()
		if err != nil {
			log.Fatal(err)
		}
	}
	{
		us, err := testunit.NewNub(4, ct, r1)
		if err != nil {
			log.Fatal(err)
		}
		r1, err = nub.New(us, ct).Nub()
		if err != nil {
			log.Fatal(err)
		}
	}
	us, err := testunit.New(4, unit.Minus, ct, r0, r1)
	if err != nil {
		log.Fatal(err)
	}
	_, err = minus.New(us, ct).Minus()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("minus process: %v\n", time.Now().Sub(t))
}
*/

func testNub(id string, db storage.Database) {
	ct := context.New()
	r, err := disk.New(id, db, ct)
	if err != nil {
		log.Fatal(err)
	}
	t := time.Now()
	us, err := testunit.NewNub(4, ct, r)
	if err != nil {
		log.Fatal(err)
	}
	_, err = nub.New(us, ct).Nub()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("nub process: %v\n", time.Now().Sub(t))
}

func testOrder(id string, db storage.Database) {
	ct := context.New()
	r, err := disk.New(id, db, ct)
	if err != nil {
		log.Fatal(err)
	}
	{
		lt := aorder.NewLT([]bool{false}, []string{"name"}, r.Metadata())
		t := time.Now()
		us, err := testunit.NewOrder(4, ct, r, lt)
		if err != nil {
			log.Fatal(err)
		}
		_, err = order.New(us, ct, lt).Order()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("order process: %v\n", time.Now().Sub(t))
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
	fmt.Printf("restrict process: %v\n", time.Now().Sub(t))
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
	fmt.Printf("summarize process: %v\n", time.Now().Sub(t))
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
	fmt.Printf("projection process: %v\n", time.Now().Sub(t))
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
