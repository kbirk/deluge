package pool

import (
	"io"
)

// Worker represents a designated worker function to batch in a pool.
type Worker func(io.Reader) error
