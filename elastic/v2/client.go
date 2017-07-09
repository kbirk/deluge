package elastic

import (
	"fmt"
	"time"

	"gopkg.in/olivere/elastic.v3"

	"github.com/unchartedsoftware/deluge"
	es "github.com/unchartedsoftware/deluge/input/elastic"
)

var (
	// SetHTTPClient can be used to specify the http.Client to use when making
	// HTTP requests to Elasticsearch.
	SetHTTPClient = elastic.SetHttpClient

	// SetBasicAuth can be used to specify the HTTP Basic Auth credentials to
	// use when making HTTP requests to Elasticsearch.
	SetBasicAuth = elastic.SetBasicAuth

	// SetURL defines the URL endpoints of the Elasticsearch nodes. Notice that
	// when sniffing is enabled, these URLs are used to initially sniff the
	// cluster on startup.
	SetURL = elastic.SetURL

	// SetScheme sets the HTTP scheme to look for when sniffing (http or https).
	// This is http by default.
	SetScheme = elastic.SetScheme

	// SetSniff enables or disables the sniffer (enabled by default).
	SetSniff = elastic.SetSniff

	// SetSnifferTimeoutStartup sets the timeout for the sniffer that is used
	// when creating a new client. The default is 5 seconds. Notice that the
	// timeout being used for subsequent sniffing processes is set with
	// SetSnifferTimeout.
	SetSnifferTimeoutStartup = elastic.SetSnifferTimeoutStartup

	// SetSnifferTimeout sets the timeout for the sniffer that finds the
	// nodes in a cluster. The default is 2 seconds. Notice that the timeout
	// used when creating a new client on startup is usually greater and can
	// be set with SetSnifferTimeoutStartup.
	SetSnifferTimeout = elastic.SetSnifferTimeout

	// SetSnifferInterval sets the interval between two sniffing processes.
	// The default interval is 15 minutes.
	SetSnifferInterval = elastic.SetSnifferInterval

	// SetHealthcheck enables or disables healthchecks (enabled by default).
	SetHealthcheck = elastic.SetHealthcheck

	// SetHealthcheckTimeoutStartup sets the timeout for the initial health check.
	// The default timeout is 5 seconds (see DefaultHealthcheckTimeoutStartup).
	// Notice that timeouts for subsequent health checks can be modified with
	// SetHealthcheckTimeout.
	SetHealthcheckTimeoutStartup = elastic.SetHealthcheckTimeoutStartup

	// SetHealthcheckTimeout sets the timeout for periodic health checks.
	// The default timeout is 1 second (see DefaultHealthcheckTimeout).
	// Notice that a different (usually larger) timeout is used for the initial
	// healthcheck, which is initiated while creating a new client.
	// The startup timeout can be modified with SetHealthcheckTimeoutStartup.
	SetHealthcheckTimeout = elastic.SetHealthcheckTimeout

	// SetHealthcheckInterval sets the interval between two health checks.
	// The default interval is 60 seconds.
	SetHealthcheckInterval = elastic.SetHealthcheckInterval

	// SetMaxRetries sets the maximum number of retries before giving up when
	// performing a HTTP request to Elasticsearch.
	SetMaxRetries = elastic.SetMaxRetries

	// SetGzip enables or disables gzip compression (disabled by default).
	SetGzip = elastic.SetGzip

	// SetDecoder sets the Decoder to use when decoding data from Elasticsearch.
	// DefaultDecoder is used by default.
	SetDecoder = elastic.SetDecoder

	// SetRequiredPlugins can be used to indicate that some plugins are required
	// before a Client will be created.
	SetRequiredPlugins = elastic.SetRequiredPlugins

	// SetErrorLog sets the logger for critical messages like nodes joining
	// or leaving the cluster or failing requests. It is nil by default.
	SetErrorLog = elastic.SetErrorLog

	// SetInfoLog sets the logger for informational messages, e.g. requests
	// and their response times. It is nil by default.
	SetInfoLog = elastic.SetInfoLog

	// SetTraceLog specifies the log.Logger to use for output of HTTP requests
	// and responses which is helpful during debugging. It is nil by default.
	SetTraceLog = elastic.SetTraceLog

	// SetSendGetBodyAs specifies the HTTP method to use when sending a GET request
	// with a body. It is GET by default.
	SetSendGetBodyAs = elastic.SetSendGetBodyAs
)

// Client represents an elasticsearch client compatible with version 2.x.x.
type Client struct {
	client *elastic.Client
}

// NewClient returns a new elasticsearch client.
func NewClient(options ...elastic.ClientOptionFunc) (*Client, error) {
	client, err := elastic.NewClient(options...)
	if err != nil {
		return nil, err
	}
	return &Client{
		client: client,
	}, nil
}

// NewBulkRequest returns a new bulk request struct.
func (c *Client) NewBulkRequest(index string) deluge.BulkRequest {
	return &BulkRequest{
		service: c.client.Bulk().Index(index),
		start:   time.Now(),
	}
}

// IndexExists returns whether or not the specified index exists.
func (c *Client) IndexExists(index string) (bool, error) {
	return c.client.IndexExists(index).Do()
}

// DeleteIndex deletes the specified index.
func (c *Client) DeleteIndex(index string) error {
	res, err := c.client.DeleteIndex(index).Do()
	if err != nil {
		return fmt.Errorf("Error occurred while deleting index: %v", err)
	}
	if !res.Acknowledged {
		return fmt.Errorf("Delete index request not acknowledged for index: `%s`", index)
	}
	return nil
}

// CreateIndex creates the specified index with the provided mapping.
func (c *Client) CreateIndex(index string, mapping string) error {
	// prepare the create index body
	body := fmt.Sprintf("{\"mappings\":%s,\"settings\":{\"number_of_replicas\":0}}", mapping)
	res, err := c.client.CreateIndex(index).Body(body).Do()
	if err != nil {
		return fmt.Errorf("Error occurred while creating index: %v", err)
	}
	if !res.Acknowledged {
		return fmt.Errorf("Create index request not acknowledged for `%s`", index)
	}
	return nil
}

// PutMapping uploads the provided mapping.
func (c *Client) PutMapping(index string, typ string, mapping string) error {
	res, err := c.client.PutMapping().Index(index).Type(typ).BodyString(mapping).Do()
	if err != nil {
		return fmt.Errorf("Error occurred while updating mapping for index: %v", err)
	}
	if !res.Acknowledged {
		return fmt.Errorf("Put mapping request not acknowledged for `%s`", index)
	}
	return nil
}

// EnableReplicas enables the provided number of replicas.
func (c *Client) EnableReplicas(index string, numReplicas int) error {
	body := fmt.Sprintf("{\"index\":{\"number_of_replicas\":%d}}", numReplicas)
	res, err := c.client.IndexPutSettings(index).BodyString(body).Do()
	if err != nil {
		return fmt.Errorf("Error occurred while enabling replicas: %v", err)
	}
	if !res.Acknowledged {
		return fmt.Errorf("Enable replication index request not acknowledged for index `%s`", index)
	}
	return nil
}

// GetIndexSummary returns an index summary struct.
func (c *Client) GetIndexSummary(index string) (es.IndexSummary, error) {
	// get stats about the index
	stats, err := c.client.IndexStats(index).Do()
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
	return &IndexSummary{
		numDocs:  uint64(numDocs),
		byteSize: uint64(byteSize),
	}, nil
}

// GetIndexReader returns an index reader struct.
func (c *Client) GetIndexReader(index string, scanSize int) (es.IndexReader, error) {
	// create the scan cursor
	cursor, err := c.client.Scan(index).Size(scanSize).Do()
	if err != nil {
		return nil, fmt.Errorf("Error occurred whiling scanning: %v", err)
	}
	return &IndexReader{
		cursor: cursor,
	}, nil
}
