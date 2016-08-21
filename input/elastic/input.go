package elastic

import (
	"fmt"
	"io"

	"github.com/unchartedsoftware/deluge"
	es "github.com/unchartedsoftware/deluge/elastic"
	"github.com/unchartedsoftware/deluge/util"
)

// Input represents an input type for reading files off a filesystem.
type Input struct {
	reader   io.Reader
	host     string
	port     string
	index    string
	numDocs  int64
	byteSize int64
}

// NewInput instantiates a new instance of a file input.
func NewInput(host, port, index string, scanSize int) (deluge.Input, error) {
	// get stats about the index
	stats, err := es.IndexStats(host, port, index)
	if err != nil {
		return nil, err
	}
	// ensure index exists
	indexStats, ok := stats.Indices[index]
	if !ok {
		return nil, fmt.Errorf("Index `%s:%s/%s` does not exist",
			host,
			port,
			index)
	}
	numDocs := int64(0)
	// ensure no nil pointers
	if indexStats.Primaries != nil &&
		indexStats.Primaries.Docs != nil {
		numDocs = indexStats.Primaries.Docs.Count
	}
	byteSize := int64(0)
	// ensure no nil pointers
	if indexStats.Primaries != nil &&
		indexStats.Primaries.Store != nil {
		byteSize = indexStats.Primaries.Store.SizeInBytes
	}
	// create the io.Reader
	reader, err := NewReader(host, port, index, scanSize)
	if err != nil {
		return nil, err
	}
	return &Input{
		reader:   reader,
		host:     host,
		port:     port,
		index:    index,
		numDocs:  numDocs,
		byteSize: byteSize,
	}, nil
}

// Next returns the io.Reader to scan the index for more docs.
func (i *Input) Next() (io.Reader, error) {
	// TODO: currently instances of this share the same ScanCursor and are
	// therefore locked with a mutex. Instead try incrementing the scan here
	// so that the resulting io.Reader can be used concurrently
	return i.reader, nil
}

// Summary returns a string containing summary information.
func (i *Input) Summary() string {
	return fmt.Sprintf("Input `%s:%s/%s` contains %d documents containing %s",
		i.host,
		i.port,
		i.index,
		i.numDocs,
		util.FormatBytes(i.byteSize))
}
