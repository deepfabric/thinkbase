package sqldriver

func (tx *sqlTx) Commit() error {
	return nil
}

func (tx *sqlTx) Rollback() error {
	return nil
}
