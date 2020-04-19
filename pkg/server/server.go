package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/deepfabric/thinkbase/pkg/logger"
	"github.com/deepfabric/thinkbase/pkg/sql/build"
	"github.com/deepfabric/thinkbase/pkg/vm/context/testContext"
	"github.com/deepfabric/thinkbase/pkg/vm/opt"
	"github.com/deepfabric/thinkbase/pkg/vm/types"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
	"github.com/valyala/fasthttp"
)

func New(port int) *server {
	return &server{port, logger.New(os.Stderr, "thinkbase")}
}

func (s *server) Run() {
	h := func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/query":
			dealQuery(s, ctx)
		default:
			ctx.Error("Unsupport Path", fasthttp.StatusNotFound)
		}
	}
	fasthttp.ListenAndServe(fmt.Sprintf(":%v", s.port), h)
}

func dealQuery(s *server, ctx *fasthttp.RequestCtx) {
	var hr HttpResult
	var mp map[string]interface{}

	ctx.Response.SetStatusCode(200)
	ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")
	ctx.Response.Header.Set("Content-Type", "application/json")
	if err := json.Unmarshal(ctx.PostBody(), &mp); err != nil {
		ctx.Response.SetStatusCode(400)
		hr.Msg = err.Error()
		data, _ := json.Marshal(hr)
		ctx.Write(data)
		return
	}
	sql, err := getString("query", mp)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		hr.Msg = fmt.Sprintf("query: %v", err)
		data, _ := json.Marshal(hr)
		ctx.Write(data)
		return
	}
	t := time.Now()
	{
		s.log.Debugf("recv query: '%s'\n", sql)
	}
	n, err := build.New(sql, testContext.New(1, 1, 1024*1024*1024, 1024*1024*1024*1024)).Build()
	if err != nil {
		ctx.Response.SetStatusCode(400)
		hr.Msg = fmt.Sprintf("deal '%s': %v", sql, err)
		data, _ := json.Marshal(hr)
		ctx.Write(data)
		return
	}
	n = opt.New(n).Optimize()
	attrs, err := n.AttributeList()
	if err != nil {
		ctx.Response.SetStatusCode(400)
		hr.Msg = fmt.Sprintf("deal '%s': %v", sql, err)
		data, _ := json.Marshal(hr)
		ctx.Write(data)
		return
	}
	hr.Attributes = attrs
	hr.Algebra = n.String()
	for {
		ts, err := n.GetTuples(1024 * 1024)
		if err != nil {
			ctx.Response.SetStatusCode(400)
			hr.Msg = fmt.Sprintf("deal '%s': %v", sql, err)
			data, _ := json.Marshal(hr)
			ctx.Write(data)
			return
		}
		for _, t := range ts {
			a := t.(value.Array)
			row := []interface{}{}
			for i := range a {
				switch a[i].ResolvedType().Oid {
				case types.T_int:
					row = append(row, value.MustBeInt(a[i]))
				case types.T_null:
					row = append(row, nil)
				case types.T_bool:
					row = append(row, value.MustBeBool(a[i]))
				case types.T_float:
					row = append(row, value.MustBeFloat(a[i]))
				default:
					row = append(row, a[i].String())
				}
			}
			hr.Rows = append(hr.Rows, row)
		}
		if len(ts) == 0 {
			break
		}
	}
	hr.Msg = "success"
	hr.ProcessTime = fmt.Sprintf("%v", time.Now().Sub(t))
	data, _ := json.Marshal(hr)
	ctx.Write(data)
}

func getString(k string, mp map[string]interface{}) (string, error) {
	v, ok := mp[k]
	if !ok {
		return "", errors.New("Not Exist")
	}
	if _, ok := v.(string); !ok {
		return "", errors.New("Not String")
	}
	return v.(string), nil
}
