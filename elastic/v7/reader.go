package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/olivere/elastic/v7"
)

// IndexReader represents an interface to read from an elasticsearch index.
type IndexReader struct {
	scroll *elastic.ScrollService
}

// Next returns the io.Reader to scan the index for more docs.
func (i *IndexReader) Next() (io.Reader, error) {
	res, err := i.scroll.Do(context.Background())
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
