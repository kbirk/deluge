package equalizer

import (
	"fmt"
	"time"

	"gopkg.in/olivere/elastic.v3"

	es "github.com/unchartedsoftware/deluge/elastic"
)

// Request represents a bulk request and its generation time.
type Request struct {
	bulk  *elastic.BulkService
	took  uint64
	reqs  []elastic.BulkableRequest
	start time.Time
}

// NewRequest creates and returns a pointer to a request object.
func NewRequest(host string, port string, index string) (*Request, error) {
	bulk, err := es.GetBulkRequest(host, port, index)
	if err != nil {
		return nil, err
	}
	return &Request{
		bulk:  bulk,
		reqs:  make([]elastic.BulkableRequest, 0),
		start: time.Now(),
	}, nil
}

// Add adds a bulkable request to the bulk payload.
func (r *Request) Add(req elastic.BulkableRequest) {
	r.reqs = append(r.reqs, req)
	r.bulk.Add(req)
}

// EstimatedSizeInBytes returns the estimated size in bytes.
func (r *Request) EstimatedSizeInBytes() int64 {
	return r.bulk.EstimatedSizeInBytes()
}

// NumberOfActions returns the number of actions.
func (r *Request) NumberOfActions() int {
	return r.bulk.NumberOfActions()
}

// Send sends the bulk request and handles the response.
func (r *Request) Send() (uint64, error) {
	// send response
	res, err := r.bulk.Do()
	if err != nil {
		return 0, err
	}
	if res.Errors {
		// find first error and return it
		for index, items := range res.Items {
			action, ok := items["index"]
			if ok {
				if action.Error != nil {
					var src = r.reqs[index].String()
					return uint64(res.Took), fmt.Errorf("%s: %s, %s", action.Error.Type, action.Error.Reason, src)
				}
			}
		}
	}
	return uint64(res.Took), nil
}

func (r *Request) stamp() {
	r.took = uint64((time.Since(r.start)).Nanoseconds()) / uint64(time.Millisecond)
}
