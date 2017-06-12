package elastic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"gopkg.in/olivere/elastic.v3"

	"github.com/unchartedsoftware/deluge/util"
)

// Input represents an input type for scanning documents out of elasticsearch.
type InputV3 struct {
	cursor   *elastic.ScanCursor
	index    string
	numDocs  int64
	byteSize int64
}

// NewInputV3 instantiates a new instance of an elasticsearch input.
func NewInputV3(client *elastic.Client, index string, scanSize int) (*InputV3, error) {
	// get stats about the index
	stats, err := client.IndexStats(index).Do()
	if err != nil {
		return nil, fmt.Errorf("Error occurred while querying index stats for `%s`: %v",
			index,
			err)
	}
	// don't access by index name, it won't work if this is an alias to an
	// index. Since we are doing a query for a specific index already, there
	// should be only one index in the response.
	if len(stats.Indices) < 1 {
		return nil, fmt.Errorf("Index `%s` does not exist", index)
	}
	// grab the first index in the map (there should only be one)
	var indexStats *elastic.IndexStats
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
	cursor, err := client.Scan(index).Size(scanSize).Do()
	if err != nil {
		return nil, fmt.Errorf("Error occurred whiling scanning: %v", err)
	}
	return &InputV3{
		cursor:   cursor,
		index:    index,
		numDocs:  numDocs,
		byteSize: byteSize,
	}, nil
}

// Next returns the io.Reader to scan the index for more docs.
func (i *InputV3) Next() (io.Reader, error) {
	res, err := i.cursor.Next()
	if err == elastic.EOS {
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
func (i *InputV3) Summary() string {
	return fmt.Sprintf("Input `%s` contains %d documents containing %s",
		i.index,
		i.numDocs,
		util.FormatBytes(i.byteSize))
}
