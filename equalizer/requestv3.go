package equalizer

import (
	"fmt"
	"time"

	"gopkg.in/olivere/elastic.v3"
)

// Request represents a bulk request and its generation time.
type RequestV3 struct {
	bulk  *elastic.BulkService
	took  uint64
	reqs  []elastic.BulkableRequest
	start time.Time
}

// NewRequestV3 creates and returns a pointer to a request object.
func NewRequestV3(client *elastic.Client, index string) *RequestV3 {
	return &RequestV3{
		bulk:  client.Bulk().Index(index),
		reqs:  make([]elastic.BulkableRequest, 0),
		start: time.Now(),
	}
}

// Add adds a bulkable request to the bulk payload.
func (r *RequestV3) Add(req elastic.BulkableRequest) {
	r.reqs = append(r.reqs, req)
	r.bulk.Add(req)
}

// EstimatedSizeInBytes returns the estimated size in bytes.
func (r *RequestV3) EstimatedSizeInBytes() int64 {
	return r.bulk.EstimatedSizeInBytes()
}

// NumberOfActions returns the number of actions.
func (r *RequestV3) NumberOfActions() int {
	return r.bulk.NumberOfActions()
}

// Send sends the bulk request and handles the response.
func (r *RequestV3) Send() (uint64, error) {
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

func (r *RequestV3) stamp() {
	r.took = uint64((time.Since(r.start)).Nanoseconds()) / uint64(time.Millisecond)
}
