package elastic

import (
	"encoding/json"
	"io"
	"sync"

	es "gopkg.in/olivere/elastic.v3"

	"github.com/unchartedsoftware/deluge/elastic"
)

// Reader represents a reader class for scanning an elasticsearch index as
// input.
type Reader struct {
	cursor *es.ScanCursor
	bytes  []byte
	done   bool
	index  int
	mu     sync.Mutex
}

// NewReader instantiates and returns an elasticsearch reader.
func NewReader(host, port, index string, scanSize int) (io.Reader, error) {
	cursor, err := elastic.Scan(host, port, index, scanSize)
	if err != nil {
		return nil, err
	}
	return &Reader{
		cursor: cursor,
		done:   false,
		index:  0,
		bytes:  make([]byte, 0),
		mu:     sync.Mutex{},
	}, nil
}

func (r *Reader) scan() error {
	res, err := r.cursor.Next()
	if err == es.EOS {
		// End of stream (or scan)
		return io.EOF
	}
	if err != nil {
		return err
	}
	if len(res.Hits.Hits) == 0 {
		return io.EOF
	}
	// reset index
	r.index = 0
	// clear buffer
	r.bytes = make([]byte, 0)
	// marhall the docs into bytes
	for _, doc := range res.Hits.Hits {
		bytes, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		// append a newline
		bytes = append(bytes, byte('\n'))
		// add to buffer
		r.bytes = append(r.bytes, bytes...)
	}
	return nil
}

// Read returns elasticsearch documents as a byte slice.
func (r *Reader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// TODO: may not need this check?
	if r.done {
		return 0, io.EOF
	}
	numBytesRead := 0
	tlen := len(r.bytes)
	plen := len(p)
	// check if we need to scan for more docs
	if r.index >= tlen {
		err := r.scan()
		// did we encounter an error?
		if err != io.EOF {
			return 0, err
		}
		// have we scanned al the docs?
		if err == io.EOF {
			r.done = true
			return 0, io.EOF
		}
	}
	// read as much data as we can
	for i := 0; r.index < tlen && i < plen; i++ {
		p[i] = r.bytes[r.index]
		r.index++
		numBytesRead++
	}
	return numBytesRead, nil
}
