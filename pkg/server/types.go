package server

import (
	"github.com/deepfabric/thinkbase/pkg/logger"
	"github.com/deepfabric/thinkbase/pkg/storage"
	"github.com/valyala/fasthttp"
)

const (
	S0 = iota // query
	S1        // insert
)

type Server interface {
	Run()
}

type HttpResult struct {
	Msg         string                   `json:"msg"`
	Algebra     string                   `json:"algebra"`
	ProcessTime string                   `json:"process time"`
	Rows        []map[string]interface{} `json:"rows"`
}

type server struct {
	port int
	log  logger.Log
	stg  storage.Storage
	srv  *fasthttp.Server
}
