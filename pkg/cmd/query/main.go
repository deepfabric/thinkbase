package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/deepfabric/thinkbase/pkg/sqldriver"
)

func main() {
	db, err := sql.Open("thinkbase", "tom:123:test@http://124.70.164.59")
	if err != nil {
		log.Fatal(err)
	}
	rows, err := db.Query(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	attrs, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}
	values := make([]sql.RawBytes, len(attrs))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	{
		for i, attr := range attrs {
			if i == 0 {
				fmt.Printf("%v", attr)
			} else {
				fmt.Printf(", %v", attr)
			}
		}
		fmt.Printf("\n")
	}
	for rows.Next() {
		if err = rows.Scan(scanArgs...); err != nil {
			log.Fatal(err)
		}
		var v string
		for i, col := range values {
			if col == nil {
				v = "NULL"
			} else {
				v = string(col)
			}
			if i == 0 {
				fmt.Printf("%v", v)
			} else {
				fmt.Printf(", %v", v)
			}
		}
		fmt.Printf("\n")
	}
}
