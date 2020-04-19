package server

import "github.com/deepfabric/thinkbase/pkg/logger"

type Server interface {
	Run()
}

type HttpResult struct {
	Msg         string          `json:"msg"`
	Algebra     string          `json:"algebra"`
	ProcessTime string          `json:"process time"`
	Attributes  []string        `json:"attribute list"`
	Rows        [][]interface{} `json:"rows"`
}

type server struct {
	port int
	log  logger.Log
}
