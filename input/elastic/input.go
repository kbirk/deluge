package elastic

import (
	"fmt"
	"io"

	"github.com/unchartedsoftware/deluge/util"
)

// Input represents an input type for scanning documents out of elasticsearch.
type Input struct {
	reader   IndexReader
	index    string
	numDocs  uint64
	byteSize uint64
}

// IndexSummary represents a summary of an elasticsearch index.
type IndexSummary interface {
	NumDocs() uint64
	ByteSize() uint64
}

// IndexReader represents an interface to read from an elasticsearch index.
type IndexReader interface {
	Next() (io.Reader, error)
}

// Client represents the elasticsearch client interface.
type Client interface {
	GetIndexSummary(string) (IndexSummary, error)
	GetIndexReader(string, int) (IndexReader, error)
}

// NewInput instantiates a new instance of an elasticsearch input.
func NewInput(client Client, index string, scanSize int) (*Input, error) {
	// get stats about the index
	summary, err := client.GetIndexSummary(index)
	if err != nil {
		return nil, err
	}
	// create the scroll service
	reader, err := client.GetIndexReader(index, scanSize)
	if err != nil {
		return nil, err
	}
	return &Input{
		reader:   reader,
		index:    index,
		numDocs:  summary.NumDocs(),
		byteSize: summary.ByteSize(),
	}, nil
}

// Next returns the io.Reader to scan the index for more docs.
func (i *Input) Next() (io.Reader, error) {
	return i.reader.Next()
}

// Summary returns a string containing summary information.
func (i *Input) Summary() string {
	return fmt.Sprintf("Input `%s` contains %d documents containing %s",
		i.index,
		i.numDocs,
		util.FormatBytes(int64(i.byteSize)))
}
