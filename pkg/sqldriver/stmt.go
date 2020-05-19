package sqldriver

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/valyala/fasthttp"
)

func (s *sqlStmt) Close() error {
	return nil
}

func (s *sqlStmt) NumInput() int {
	return -1
}

func (s *sqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	switch strings.ToLower(s.query) {
	case "use":
		return s.dealUse(args)
	case "insert":
		return s.dealInsert(args)
	}
	return nil, nil
}

func (s *sqlStmt) Query(_ []driver.Value) (driver.Rows, error) {
	var req fasthttp.Request
	var resp fasthttp.Response

	defer fasthttp.ReleaseRequest(&req)
	defer fasthttp.ReleaseResponse(&resp)
	req.Header.SetMethod("POST")
	{
		body, err := json.Marshal(&Query{
			Uid:      s.c.uid,
			Query:    s.query,
			Database: s.c.database,
		})
		if err != nil {
			return nil, err
		}
		req.SetBody(body)
	}
	req.SetRequestURI(s.c.url + "/query")
	req.Header.Add("Content-Type", "application/json")
	if err := fasthttp.Do(&req, &resp); err != nil {
		return nil, err
	}
	return parseRows(resp.Body())
}

// use database
func (s *sqlStmt) dealUse(args []driver.Value) (driver.Result, error) {
	if len(args) < 1 {
		return nil, errors.New("usage: use database")
	}
	if v, ok := args[0].(string); ok {
		s.c.database = v
		return &sqlResult{err: nil}, nil
	}
	return nil, fmt.Errorf("args[0] '%v' is not string", args[0])
}

// insert table data [metadata]
func (s *sqlStmt) dealInsert(args []driver.Value) (driver.Result, error) {
	switch len(args) {
	case 2:
		var i Insert

		i.Uid = s.c.uid
		i.Database = s.c.database
		if v, ok := args[0].(string); !ok {
			return nil, fmt.Errorf("args[0] '%v' is not string", args[0])
		} else {
			i.Table = v
		}
		if v, ok := args[1].([]byte); !ok {
			return nil, fmt.Errorf("args[1] '%v' is not []byte", args[1])
		} else {
			i.Data = v
		}
		if data, err := json.Marshal(&i); err != nil {
			return nil, err
		} else {
			return s.insert(data)
		}
	case 3:
		var i InsertWithMetadata

		i.Uid = s.c.uid
		i.Database = s.c.database
		if v, ok := args[0].(string); !ok {
			return nil, fmt.Errorf("args[0] '%v' is not string", args[0])
		} else {
			i.Table = v
		}
		if v, ok := args[1].([]byte); !ok {
			return nil, fmt.Errorf("args[1] '%v' is not []byte", args[1])
		} else {
			i.Data = v
		}
		if v, ok := args[2].([]byte); !ok {
			return nil, fmt.Errorf("args[2] '%v' is not []byte", args[2])
		} else {
			i.MetaData = v
		}
		if data, err := json.Marshal(&i); err != nil {
			return nil, err
		} else {
			return s.insert(data)
		}
	}
	return nil, errors.New("usage: insert table data [metadata]")
}

func (s *sqlStmt) insert(body []byte) (driver.Result, error) {
	var req fasthttp.Request
	var resp fasthttp.Response

	defer fasthttp.ReleaseRequest(&req)
	defer fasthttp.ReleaseResponse(&resp)
	req.Header.SetMethod("POST")
	req.SetBody(body)
	req.SetRequestURI(s.c.url + "/insert")
	req.Header.Add("Content-Type", "application/json")
	if err := fasthttp.Do(&req, &resp); err != nil {
		return nil, err
	}
	return parseResult(resp.Body())
}

func parseResult(data []byte) (driver.Result, error) {
	var mp map[string]interface{}

	if err := json.Unmarshal(data, &mp); err != nil {
		return nil, err
	}
	{
		v, err := getString("msg", mp)
		if err != nil {
			return nil, err
		}
		if v != "success" {
			return nil, errors.New(v)
		}
	}
	return &sqlResult{err: nil}, nil
}

func parseRows(data []byte) (driver.Rows, error) {
	var r sqlRows
	var mp map[string]interface{}

	if err := json.Unmarshal(data, &mp); err != nil {
		return nil, err
	}
	{
		v, err := getString("msg", mp)
		if err != nil {
			return nil, err
		}
		if v != "success" {
			return nil, errors.New(v)
		}
	}
	{
		v, ok := mp["rows"]
		if !ok {
			return nil, errors.New("rows: Not Exist")
		}
		xs, ok := v.([]interface{})
		if !ok {
			return nil, errors.New("rows: Not Array")
		}
		for i, j := 0, len(xs); i < j; i++ {
			if t, ok := xs[i].(map[string]interface{}); ok {
				if len(r.ts) == 0 {
					for k, _ := range t {
						r.attrs = append(r.attrs, k)
					}
				}
				r.ts = append(r.ts, t)
			}
		}
	}
	return &r, nil
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
