package elastic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	e "gopkg.in/olivere/elastic.v3"

	"github.com/unchartedsoftware/deluge"
	es "github.com/unchartedsoftware/deluge/elastic"
	"github.com/unchartedsoftware/deluge/util"
)

// Input represents an input type for reading files off a filesystem.
type Input struct {
	cursor   *e.ScanCursor
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
	// don't access by index name, it won't work if this is an alias to an
	// index. Since we are doing a query for a specific index already, there
	// should be only one index in the response.
	if len(stats.Indices) < 1 {
		return nil, fmt.Errorf("Index `%s:%s/%s` does not exist",
			host,
			port,
			index)
	}
	// grab the first index in the map (there should only be one)
	var indexStats *e.IndexStats
	for _, value := range stats.Indices {
		indexStats = value
		break
	}
	// get number of documents
	numDocs := int64(0)
	// ensure no nil pointers
	if indexStats.Primaries != nil &&
		indexStats.Primaries.Docs != nil {
		numDocs = indexStats.Primaries.Docs.Count
	}
	// get the btye size
	byteSize := int64(0)
	// ensure no nil pointers
	if indexStats.Primaries != nil &&
		indexStats.Primaries.Store != nil {
		byteSize = indexStats.Primaries.Store.SizeInBytes
	}
	// create the scan cursor
	cursor, err := es.Scan(host, port, index, scanSize)
	if err != nil {
		return nil, err
	}
	return &Input{
		cursor:   cursor,
		host:     host,
		port:     port,
		index:    index,
		numDocs:  numDocs,
		byteSize: byteSize,
	}, nil
}

// Next returns the io.Reader to scan the index for more docs.
func (i *Input) Next() (io.Reader, error) {
	res, err := i.cursor.Next()
	if err == e.EOS {
		// End of stream (or scan)
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}
	if len(res.Hits.Hits) == 0 {
		return nil, io.EOF
	}
	// create buffer
	var buffer []byte
	// marhall the docs into bytes
	for _, doc := range res.Hits.Hits {
		sub, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}
		// append a newline
		sub = append(sub, byte('\n'))
		// add to buffer
		buffer = append(buffer, sub...)
	}
	return bytes.NewReader(buffer), nil
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
