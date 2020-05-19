package container

import "errors"

var (
	Empty     = errors.New("empty")
	NotExist  = errors.New("Not Exist")
	OutOfSize = errors.New("Out of Size")
)
