package input

import (
	"errors"
)

var (
	// ErrEOS represents the end of an input stream.
	ErrEOS = errors.New("EOS")
)
