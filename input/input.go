package input

import (
	"errors"
)

var (
	// ErrEOS represents the end of an input stream.
	ErrEOS = errors.New("EOS")
)

// Source represents a single input to ingest.
type Source struct {
	Name string
	Path string
	Size uint64
}

// Info represents a batch of ingestible data.
type Info struct {
	Sources       []*Source
	NumTotalBytes uint64
}

// Input represents an input type for reading files.
type Input interface {
	GetInfo(string) (*Info, error)
	GetPath() string
	Next() (interface{}, error)
}
