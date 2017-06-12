package deluge

import (
	"io"

	h "github.com/colinmarc/hdfs"
	es3 "gopkg.in/olivere/elastic.v3"
	es5 "gopkg.in/olivere/elastic.v5"

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
func NewElasticInputV3(client *es3.Client, index string, scanSize int) (Input, error) {
	return elastic.NewInputV3(client, index, scanSize)
}

// NewElasticInput instantiates a new instance of an elasticsearch input.
func NewElasticInput(client *es5.Client, index string, scanSize int) (Input, error) {
	return elastic.NewInput(client, index, scanSize)
}

// NewFileInput instantiates a new instance of a file input.
func NewFileInput(path string, excludes []string) (Input, error) {
	return file.NewInput(path, excludes)
}

// NewHDFSInput instantiates a new instance of a hdfs input.
func NewHDFSInput(client *h.Client, path string, excludes []string) (Input, error) {
	return hdfs.NewInput(client, path, excludes)
}
