package deluge

import (
	"github.com/unchartedsoftware/deluge/elastic"
	"github.com/unchartedsoftware/deluge/input"
)

// IngestorOptionFunc is a function that configures an Ingestor. It is used in
// NewIngestor.
type IngestorOptionFunc func(*Ingestor) error

// SetDocument sets the document type for the ingest.
func SetDocument(document elastic.Document) IngestorOptionFunc {
	return func(i *Ingestor) error {
		if document != nil {
			i.document = document
		}
		return nil
	}
}

// SetInput sets the input type for the ingest.
func SetInput(input input.Input) IngestorOptionFunc {
	return func(i *Ingestor) error {
		if input != nil {
			i.input = input
		}
		return nil
	}
}

// SetURL sets the elasticsearch endpoint url.
func SetURL(host, port string) IngestorOptionFunc {
	return func(i *Ingestor) error {
		i.host = host
		i.port = port
		return nil
	}
}

// SetErrorThreshold sets the error threshold to stop the ingest at.
func SetErrorThreshold(threshold float64) IngestorOptionFunc {
	return func(i *Ingestor) error {
		i.threshold = threshold
		return nil
	}
}

// SetActiveConnections sets the number of active connections to elasticsearch
// allowed.
func SetActiveConnections(numActiveConnections int) IngestorOptionFunc {
	return func(i *Ingestor) error {
		i.numActiveConnections = numActiveConnections
		return nil
	}
}

// SetNumReplicas sets the number of replicas to enable upon completion of
// the ingest.
func SetNumReplicas(numReplicas int) IngestorOptionFunc {
	return func(i *Ingestor) error {
		i.numReplicas = numReplicas
		return nil
	}
}

// SetNumWorkers sets the number of workers in the worker pool.
func SetNumWorkers(numWorkers int) IngestorOptionFunc {
	return func(i *Ingestor) error {
		i.numWorkers = numWorkers
		return nil
	}
}

// ClearExisting clears an existing index if specified.
func ClearExisting(clearExisting bool) IngestorOptionFunc {
	return func(i *Ingestor) error {
		i.clearExisting = clearExisting
		return nil
	}
}

// SetCompression sets the compression type for the input files.
func SetCompression(compression string) IngestorOptionFunc {
	return func(i *Ingestor) error {
		i.compression = compression
		return nil
	}
}

// SetIndex sets the index name to create and ingest into.
func SetIndex(index string) IngestorOptionFunc {
	return func(i *Ingestor) error {
		i.index = index
		return nil
	}
}
