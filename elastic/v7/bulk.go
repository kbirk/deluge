package elastic

import (
	"context"
	"fmt"
	"time"

	"github.com/olivere/elastic/v7"
)

// BulkRequest represents an elasticsearch bulk request.
type BulkRequest struct {
	service *elastic.BulkService
	reqs    []elastic.BulkableRequest
	start   time.Time
}

// Add adds a bulkable request to the bulk payload.
func (r *BulkRequest) Add(typ string, id string, source interface{}) {
	req := elastic.NewBulkIndexRequest().Id(id).Type(typ).Doc(source)
	r.service.Add(req)
	r.reqs = append(r.reqs, req)
}

// EstimatedSizeInBytes returns the estimated size in bytes.
func (r *BulkRequest) EstimatedSizeInBytes() int64 {
	return r.service.EstimatedSizeInBytes()
}

// Size returns the number of documents.
func (r *BulkRequest) Size() int {
	return len(r.reqs)
}

// Took returns the time it took to generate the request.
func (r *BulkRequest) Took() uint64 {
	return uint64((time.Since(r.start)).Nanoseconds()) / uint64(time.Millisecond)
}

// Send sends the bulk request and handles the response.
func (r *BulkRequest) Send() (uint64, error) {
	res, err := r.service.Do(context.Background())
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
