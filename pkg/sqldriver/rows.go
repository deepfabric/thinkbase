package sqldriver

import (
	"database/sql/driver"
	"io"
)

func (r *sqlRows) Close() error {
	return nil
}

func (r *sqlRows) Columns() []string {
	return r.attrs
}

func (r *sqlRows) Next(dest []driver.Value) error {
	if len(r.ts) == 0 {
		return io.EOF
	}
	t := r.ts[0]
	r.ts = r.ts[1:]
	for i, k := range r.attrs {
		dest[i] = t[k]
	}
	return nil
}
