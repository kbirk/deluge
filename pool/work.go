package pool

import (
	"io"
)

// Work represents work that is accepted by a worker.
type Work interface {
	Next() (io.Reader, error)
}
