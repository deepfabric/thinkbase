package sqldriver

import "encoding/json"

type sqlDriver struct {
}

type sqlConn struct {
	uid      string
	url      string
	database string
	password string
}

type sqlTx struct {
	c *sqlConn
}

type sqlStmt struct {
	query string
	c     *sqlConn
}

type sqlRows struct {
	attrs []string
	ts    []map[string]interface{}
}

type sqlResult struct {
	err          error
	rowsAffected int64
	lastInsertId int64
}

type Query struct {
	Uid      string `json:"uid"`
	Query    string `json:"query"`
	Database string `json:"database"`
}

type Insert struct {
	Uid      string          `json:"uid"`
	Table    string          `json:"table"`
	Database string          `json:"database"`
	Data     json.RawMessage `json:"data"`
}

type InsertWithMetadata struct {
	Uid      string          `json:"uid"`
	Table    string          `json:"table"`
	Database string          `json:"database"`
	Data     json.RawMessage `json:"data"`
	MetaData json.RawMessage `json:"metadata"`
}
