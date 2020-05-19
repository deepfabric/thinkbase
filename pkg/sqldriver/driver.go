package sqldriver

import (
	"database/sql"
	"database/sql/driver"
	"strings"
)

func init() {
	sql.Register("thinkbase", &sqlDriver{})
}

// name = uid:password:database@url
func (d *sqlDriver) Open(name string) (driver.Conn, error) {
	ys := strings.Split(name, "@")
	xs := strings.Split(ys[0], ":")
	return &sqlConn{
		uid:      xs[0],
		url:      ys[1],
		database: xs[2],
		password: xs[1],
	}, nil
}

func (c *sqlConn) Close() error {
	return nil
}

func (c *sqlConn) Begin() (driver.Tx, error) {
	return &sqlTx{c}, nil
}

func (c *sqlConn) Prepare(query string) (driver.Stmt, error) {
	return &sqlStmt{query, c}, nil
}
