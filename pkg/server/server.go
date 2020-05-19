package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/deepfabric/thinkbase/pkg/logger"
	"github.com/deepfabric/thinkbase/pkg/sql/build"
	"github.com/deepfabric/thinkbase/pkg/storage"
	"github.com/deepfabric/thinkbase/pkg/vm/context"
	"github.com/deepfabric/thinkbase/pkg/vm/estimator"
	"github.com/deepfabric/thinkbase/pkg/vm/opt"
	"github.com/deepfabric/thinkbase/pkg/vm/types"
	"github.com/deepfabric/thinkbase/pkg/vm/value"
	"github.com/deepfabric/thinkbase/pkg/vm/workspace"
	"github.com/valyala/fasthttp"
)

func New(port int, log logger.Log, stg storage.Storage) *server {
	return &server{
		log:  log,
		stg:  stg,
		port: port,
	}
}

func (s *server) Run() {
	s.srv = &fasthttp.Server{Handler: func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/query":
			dealRequest(S0, s, ctx)
		case "/insert":
			dealRequest(S1, s, ctx)
		default:
			ctx.Error("Unsupport Path", fasthttp.StatusNotFound)
		}
	}}
	s.srv.ListenAndServe(fmt.Sprintf(":%v", s.port))
}

func (s *server) dealQuery(mp map[string]interface{}, ctx *fasthttp.RequestCtx) {
	var hr HttpResult

	sql, err := getString("query", mp)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		hr.Msg = fmt.Sprintf("query: %v", err)
		data, _ := json.Marshal(hr)
		ctx.Write(data)
		return
	}
	uid, err := getString("uid", mp)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		hr.Msg = fmt.Sprintf("uid: %v", err)
		data, _ := json.Marshal(hr)
		ctx.Write(data)
		return
	}
	database, err := getString("database", mp)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		hr.Msg = fmt.Sprintf("database: %v", err)
		data, _ := json.Marshal(hr)
		ctx.Write(data)
		return
	}
	c := context.New(context.NewConfig(uid), estimator.New(), workspace.New(uid, database, s.stg))
	n, err := build.New(sql, c).Build()
	if err != nil {
		ctx.Response.SetStatusCode(400)
		hr.Msg = fmt.Sprintf("failed to parse '%s': %v", sql, err)
		data, _ := json.Marshal(hr)
		ctx.Write(data)
		return
	}
	n = opt.New(n, c).Optimize()
	attrs, err := n.AttributeList()
	if err != nil {
		ctx.Response.SetStatusCode(400)
		hr.Msg = fmt.Sprintf("failed to get attribute list: %v", err)
		data, _ := json.Marshal(hr)
		ctx.Write(data)
		return
	}
	hr.Algebra = n.String()
	t := time.Now()
	bs := c.BlockSize()
	for {
		mp, err := n.GetAttributes(attrs, bs)
		if err != nil {
			ctx.Response.SetStatusCode(400)
			hr.Msg = fmt.Sprintf("failed to process '%s': %v", sql, err)
			data, _ := json.Marshal(hr)
			ctx.Write(data)
			return
		}
		if len(mp) == 0 || len(mp[attrs[0]]) == 0 {
			break
		}
		for i, j := 0, len(mp[attrs[0]]); i < j; i++ {
			row := make(map[string]interface{})
			for _, attr := range attrs {
				v := mp[attr][i]
				switch v.ResolvedType().Oid {
				case types.T_int:
					row[attr] = value.MustBeInt(v)
				case types.T_null:
					row[attr] = nil
				case types.T_bool:
					row[attr] = value.MustBeBool(v)
				case types.T_empty:
					row[attr] = nil
				case types.T_float:
					row[attr] = value.MustBeFloat(v)
				default:
					row[attr] = v.String()
				}
			}
			hr.Rows = append(hr.Rows, row)
		}
	}
	hr.Msg = "success"
	hr.ProcessTime = fmt.Sprintf("%v", time.Now().Sub(t))
	data, _ := json.Marshal(hr)
	ctx.Write(data)
	return
}

func (s *server) dealInsert(mp map[string]interface{}, ctx *fasthttp.RequestCtx) {
	var hr HttpResult

	uid, err := getString("uid", mp)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		hr.Msg = fmt.Sprintf("uid: %v", err)
		data, _ := json.Marshal(hr)
		ctx.Write(data)
		return
	}
	database, err := getString("database", mp)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		hr.Msg = fmt.Sprintf("database: %v", err)
		data, _ := json.Marshal(hr)
		ctx.Write(data)
		return
	}
	table, err := getString("table", mp)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		hr.Msg = fmt.Sprintf("table: %v", err)
		data, _ := json.Marshal(hr)
		ctx.Write(data)
		return
	}
	ts, err := getTuples(mp)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		hr.Msg = err.Error()
		data, _ := json.Marshal(hr)
		ctx.Write(data)
		return
	}
	r, err := s.stg.Relation(uid + "." + database + "." + table)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		hr.Msg = fmt.Sprintf("failed to get relation: %v", err)
		data, _ := json.Marshal(hr)
		ctx.Write(data)
		return
	}
	t := time.Now()
	if err := r.AddTuplesByJson(ts); err != nil {
		ctx.Response.SetStatusCode(400)
		hr.Msg = fmt.Sprintf("failed to insert tuples: %v", err)
		data, _ := json.Marshal(hr)
		ctx.Write(data)
		return
	}
	hr.Msg = "success"
	hr.ProcessTime = fmt.Sprintf("%v", time.Now().Sub(t))
	data, _ := json.Marshal(hr)
	ctx.Write(data)
	return
}

func dealRequest(typ int, s *server, ctx *fasthttp.RequestCtx) {
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
	switch typ {
	case S0:
		s.dealQuery(mp, ctx)
	case S1:
		s.dealInsert(mp, ctx)
	}
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

type typecast struct {
	k   string
	typ string
}

func getTuples(mp map[string]interface{}) ([]map[string]interface{}, error) {
	var mq map[string]interface{}
	var ts []map[string]interface{}

	{
		v, ok := mp["data"]
		if !ok {
			return nil, errors.New("data: Not Exist")
		}
		xs, ok := v.([]interface{})
		if !ok {
			return nil, errors.New("data: Not Array")
		}
		for i, j := 0, len(xs); i < j; i++ {
			if t, ok := xs[i].(map[string]interface{}); ok {
				ts = append(ts, t)
			}
		}
	}
	{
		v, ok := mp["metadata"]
		if !ok {
			return ts, nil
		}
		mq, ok = v.(map[string]interface{})
		if !ok {
			return ts, nil
		}
	}
	var tcs []*typecast
	for k, v := range mq {
		if typ, ok := v.(string); ok {
			tcs = append(tcs, &typecast{k, typ})
		}
	}
	for _, t := range ts {
		for _, tc := range tcs {
			if v, ok := t[tc.k]; ok {
				t[tc.k] = typeCast(v, tc.typ)
			}
		}
	}
	return ts, nil
}

func typeCast(v interface{}, typ string) interface{} {
	switch typ {
	case "int":
		switch t := v.(type) {
		case bool:
			if t {
				return int64(1)
			}
			return int64(0)
		case string:
			if nv, err := strconv.ParseInt(t, 0, 64); err == nil {
				return nv
			}
		case float64:
			return int64(t)
		}
	case "bool":
		switch t := v.(type) {
		case string:
			if t = strings.TrimSpace(t); len(t) >= 1 {
				switch t[0] {
				case 't', 'T':
					if isCaseInsensitivePrefix(t, "true") {
						return true
					}
				case 'f', 'F':
					if isCaseInsensitivePrefix(t, "false") {
						return false
					}
				}
			}
		case float64:
			return t != 0
		}
	case "time":
		switch t := v.(type) {
		case string:
			if nv, err := time.Parse(value.TimeOutputFormat, t); err == nil {
				return nv
			}
		case float64:
			return time.Unix(int64(t), 0)
		}
	case "float":
		switch t := v.(type) {
		case bool:
			if t {
				return float64(1)
			}
			return float64(0)
		case string:
			if nv, err := strconv.ParseFloat(t, 64); err == nil {
				return nv
			}
		}
	case "string":
		if v == nil {
			return "null"
		}
		return fmt.Sprintf("%s", v)
	}
	return v
}

func isCaseInsensitivePrefix(prefix, s string) bool {
	if len(prefix) > len(s) {
		return false
	}
	return strings.EqualFold(prefix, s[:len(prefix)])
}
