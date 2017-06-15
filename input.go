package deluge

import (
	"io"

	"github.com/unchartedsoftware/deluge/input/elastic"
	"github.com/unchartedsoftware/deluge/input/file"
	"github.com/unchartedsoftware/deluge/input/hdfs"
)

// Input represents an input type for processing.
type Input interface {
	Next() (io.Reader, error)
	Summary() string
}

// NewElasticInput instantiates a new instance of an elasticsearch input.
func NewElasticInput(client elastic.Client, index string, scanSize int) (Input, error) {
	return elastic.NewInput(client, index, scanSize)
}

// NewFileInput instantiates a new instance of a file input.
func NewFileInput(path string, excludes []string) (Input, error) {
	return file.NewInput(path, excludes)
}

// NewHDFSInput instantiates a new instance of a hdfs input.
func NewHDFSInput(client hdfs.Client, path string, excludes []string) (Input, error) {
	return hdfs.NewInput(client, path, excludes)
}
