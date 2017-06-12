package deluge

import (
	"gopkg.in/olivere/elastic.v3"
)

// IngestorOptionFunc is a function that configures an Ingestor. It is used in
// NewIngestor.
type IngestorOptionFuncV3 func(*IngestorV3) error

// SetDocument sets the document type for the ingest.
func SetDocumentV3(ctor Constructor) IngestorOptionFuncV3 {
	return func(i *IngestorV3) error {
		if ctor != nil {
			i.documentCtor = ctor
		}
		return nil
	}
}

// SetInput sets the input type for the ingest.
func SetInputV3(input Input) IngestorOptionFuncV3 {
	return func(i *IngestorV3) error {
		if input != nil {
			i.input = input
		}
		return nil
	}
}

// SetClient sets the elasticsearch client.
func SetClientV3(client *elastic.Client) IngestorOptionFuncV3 {
	return func(i *IngestorV3) error {
		i.client = client
		return nil
	}
}

// SetErrorThreshold sets the error threshold to stop the ingest at.
func SetErrorThresholdV3(threshold float64) IngestorOptionFuncV3 {
	return func(i *IngestorV3) error {
		i.threshold = threshold
		return nil
	}
}

// SetActiveConnections sets the number of active connections to elasticsearch
// allowed.
func SetActiveConnectionsV3(numActiveConnections int) IngestorOptionFuncV3 {
	return func(i *IngestorV3) error {
		i.numActiveConnections = numActiveConnections
		return nil
	}
}

// SetNumReplicas sets the number of replicas to enable upon completion of
// the ingest.
func SetNumReplicasV3(numReplicas int) IngestorOptionFuncV3 {
	return func(i *IngestorV3) error {
		i.numReplicas = numReplicas
		return nil
	}
}

// SetNumWorkers sets the number of workers in the worker pool.
func SetNumWorkersV3(numWorkers int) IngestorOptionFuncV3 {
	return func(i *IngestorV3) error {
		i.numWorkers = numWorkers
		return nil
	}
}

// ClearExistingIndex clears an existing index if specified.
func ClearExistingIndexV3(clearExisting bool) IngestorOptionFuncV3 {
	return func(i *IngestorV3) error {
		i.clearExisting = clearExisting
		return nil
	}
}

// SetCompression sets the compression type for the input files. Supports:
// "bzip2", "flate", "gzip", "zlib".
func SetCompressionV3(compression string) IngestorOptionFuncV3 {
	return func(i *IngestorV3) error {
		i.compression = compression
		return nil
	}
}

// SetIndex sets the index name to create and ingest into.
func SetIndexV3(index string) IngestorOptionFuncV3 {
	return func(i *IngestorV3) error {
		i.index = index
		return nil
	}
}

// SetBulkByteSize sets the maximum number of bytes in a bulk index payload.
func SetBulkByteSizeV3(numBytes int64) IngestorOptionFuncV3 {
	return func(i *IngestorV3) error {
		i.bulkByteSize = numBytes
		return nil
	}
}

// SetScanBufferSize sets the maximum number of bytes in the bufio.Scanner
// a bulk index payload.
func SetScanBufferSizeV3(numBytes int) IngestorOptionFuncV3 {
	return func(i *IngestorV3) error {
		i.scanBufferSize = numBytes
		return nil
	}
}
