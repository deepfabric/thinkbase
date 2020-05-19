package sqldriver

func (r *sqlResult) LastInsertId() (int64, error) {
	return r.lastInsertId, r.err
}

func (r *sqlResult) RowsAffected() (int64, error) {
	return r.rowsAffected, r.err
}
