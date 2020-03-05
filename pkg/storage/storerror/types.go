package storerror

import "errors"

var (
	NotExist              = errors.New("Not Exist")
	CannotOpenSystemTable = errors.New("cannot open system table")
)
