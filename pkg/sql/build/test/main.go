package main

import (
	"fmt"
	"os"

	"github.com/deepfabric/thinkbase/pkg/sql/build"
)

func main() {
	root, err := build.New(os.Args[1]).Build()
	fmt.Printf("root: %v, %v\n", root, err)
}
