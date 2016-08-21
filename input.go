package deluge

import (
	"io"
)

// Input represents an input type for reading files.
type Input interface {
	Next() (io.Reader, error)
	Summary() string
}
