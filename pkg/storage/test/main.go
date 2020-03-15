package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/deepfabric/thinkbase/pkg/algebra/extend"
	"github.com/deepfabric/thinkbase/pkg/algebra/extend/overload"
	aorder "github.com/deepfabric/thinkbase/pkg/algebra/order"
	aprojection "github.com/deepfabric/thinkbase/pkg/algebra/projection"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation"
	"github.com/deepfabric/thinkbase/pkg/algebra/relation/disk"
	asummarize "github.com/deepfabric/thinkbase/pkg/algebra/summarize"
	aoverload "github.com/deepfabric/thinkbase/pkg/algebra/summarize/overload"
	"github.com/deepfabric/thinkbase/pkg/algebra/util"
	"github.com/deepfabric/thinkbase/pkg/algebra/value"
	"github.com/deepfabric/thinkbase/pkg/context"
	"github.com/deepfabric/thinkbase/pkg/exec/intersect"
	"github.com/deepfabric/thinkbase/pkg/exec/minus"
	"github.com/deepfabric/thinkbase/pkg/exec/nub"
	"github.com/deepfabric/thinkbase/pkg/exec/order"
	"github.com/deepfabric/thinkbase/pkg/exec/projection"
	"github.com/deepfabric/thinkbase/pkg/exec/restrict"
	"github.com/deepfabric/thinkbase/pkg/exec/summarize"
	"github.com/deepfabric/thinkbase/pkg/exec/testunit"
	"github.com/deepfabric/thinkbase/pkg/exec/unit"
	"github.com/deepfabric/thinkbase/pkg/storage"
	"github.com/deepfabric/thinkbasekv/pkg/engine"
	"github.com/deepfabric/thinkbasekv/pkg/engine/pb"
	"github.com/deepfabric/thinkbasekv/pkg/engine/pb/s3"
)

func main() {
	db, err := storage.New(newEngine("test-infinivision"))
	if err != nil {
		log.Fatal(err)
	}
	tbl0, err := db.Table("test0")
	if err != nil {
		log.Fatal(err)
	}
	tbl1, err := db.Table("test1")
	if err != nil {
		log.Fatal(err)
	}
	{
		if err := inject(tbl0, 10); err != nil {
			log.Fatal(err)
		}
		if err := inject(tbl1, 10); err != nil {
			log.Fatal(err)
		}
	}
	{
		printTable("test0", db)
		printTable("test1", db)
	}
	{
		fmt.Printf("++++++inject end++++++\n")
	}
	testNub("test0", db)
	testOrder("test0", db)
	testSummarize("test0", db)
	testRestrict("test0", db)
	testProjection("test0", db)
	testMinus("test0", "test1", db)
	testIntersect("test0", "test1", db)
}

func printTable(id string, db storage.Database) {
	ct := context.New()
	r, err := disk.New(id, db, ct)
	if err != nil {
		log.Fatal(err)
	}
	rr, err := util.Dup(r, ct)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%v\n", rr)
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
	rr, err := minus.New(us, ct).Minus()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("minus process: %v\n", time.Now().Sub(t))
	fmt.Printf("\t%v\n", rr)
}

func testIntersect(id0, id1 string, db storage.Database) {
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
		us, err := testunit.NewNub(8, ct, r0)
		if err != nil {
			log.Fatal(err)
		}
		r0, err = nub.New(us, ct).Nub()
		if err != nil {
			log.Fatal(err)
		}
	}
	{
		us, err := testunit.NewNub(8, ct, r1)
		if err != nil {
			log.Fatal(err)
		}
		r1, err = nub.New(us, ct).Nub()
		if err != nil {
			log.Fatal(err)
		}
	}
	us, err := testunit.New(8, unit.Intersect, ct, r0, r1)
	if err != nil {
		log.Fatal(err)
	}
	rr, err := intersect.New(us, ct).Intersect()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("intersect process: %v\n", time.Now().Sub(t))
	fmt.Printf("\t%v\n", rr)
}

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
	rr, err := nub.New(us, ct).Nub()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("nub process: %v\n", time.Now().Sub(t))
	fmt.Printf("\t%v\n", rr)
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
		rr, err := order.New(us, ct, lt).Order()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("order process: %v\n", time.Now().Sub(t))
		fmt.Printf("\t%v\n", rr)
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
	rr, err := restrict.New(us, ct).Restrict()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("restrict process: %v\n", time.Now().Sub(t))
	fmt.Printf("\t%v\n", rr)
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
	rr, err := summarize.New(ops, gs, attrs, ct, r, us).Summarize()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("summarize process: %v\n", time.Now().Sub(t))
	fmt.Printf("\t%v\n", rr)
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
	rr, err := projection.New(us, ct).Projection()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("projection process: %v\n", time.Now().Sub(t))
	fmt.Printf("\t%v\n", rr)
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

func newEngine(name string) engine.DB {
	return pb.New(name, nil)
}

func newfs() vfs.FS {
	endpoint := "http://oss-cn-hangzhou.aliyuncs.com"
	accessKeyID := ""
	accessKeySecret := ""
	acl := s3.PublicReadWrite
	fs, err := s3.New(endpoint, accessKeyID, accessKeySecret, acl)
	if err != nil {
		log.Fatal(err)
	}
	return fs
}
