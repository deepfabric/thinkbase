package main

import "github.com/deepfabric/thinkbase/pkg/server"

func main() {
	server.New(80).Run()
}
