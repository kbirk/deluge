# Deluge

> Fast and sustainable bulk Elasticsearch ingestion, migration, and cloning

## Dependencies

Requires the [Go](https://golang.org/) programming language binaries with the `GOPATH` environment variable specified.

## Installation

##### Using `go get`:

If your project does not use the vendoring tool [Glide](https://glide.sh) to manage dependencies, you can install this package like you would any other:

```bash
go get github.com/unchartedsoftware/deluge
```

While this is the simplest way to install the package, due to how `go get` resolves transitive dependencies it may result in version incompatibilities.

##### Using `glide get`:

This is the recommended way to install the package and ensures all transitive dependencies are resolved to their compatible versions.

```bash
glide get github.com/unchartedsoftware/deluge
```

NOTE: Requires [Glide](https://glide.sh) along with [Go](https://golang.org/) version 1.6+, or version 1.5 with the `GO15VENDOREXPERIMENT` environment variable set to `1`.

## Usage

This package provides facilities for customizable bulk ingests of data into [elasticsearch](https://github.com/elastic/elasticsearch).

## Example

##### Implement the `elastic.Document` interface:

```go
package sample

import (
	"github.com/unchartedsoftware/deluge/document"
	"github.com/unchartedsoftware/deluge/elastic"
)

// Document represents a simple CSV type of document.
type Document struct {
	document.CSV
}

func NewDocument() elastic.Document{
	return Document{}
}

// GetID returns the document's id.
func (d Document) GetID() (string, error) {
	return d.Cols[0], nil
}

// GetType returns the document's type.
func (d Document) GetType() string {
	return "datum"
}

// GetMapping returns the document's mapping.
func (d Document) GetMapping() (string, error) {
	return `{
        "` + d.GetType() + `": {
			"properties":{
	            "description": {
	                "type": "string"
	            }
			}
        }
    }`, nil
}

// GetSource returns the source portion of the document.
func (d Document) GetSource() (interface{}, error) {
	return map[string]interface{}{
		"description": d.Cols[1],
	}, nil
}
```

##### Use the `deluge.Ingestor` to bulk ingest:

```go
package main

import (
	"runtime"
	"github.com/unchartedsoftware/deluge"
	"github.com/username/example/sample"
)

func main() {

	// Use all CPUs
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Create the document type
	document := sample.NewDocument()

	// Create the input type
	input := file.NewInput(
		"/path/to/data",
		[ "files", "or", "dirs", "to", "exclude" ])

	// Create the ingestor object
	ingestor, err := deluge.NewIngestor(
		deluge.SetDocument(document)
		deluge.SetInput(input),
		deluge.SetIndex("test_index"),
		deluge.SetURL("10.64.16.120:9200"),
		deluge.SetErrorThreshold(0.05),
		deluge.SetActiveConnections(8),
		deluge.SetNumWorkers(8),
		deluge.SetCompression("gzip"),
		deluge.ClearExisting(),
		deluge.SetBulkByteSize(1024*1024*20),
		deluge.SetNumReplicas(1)),
	if err != nil {
		return err
	}

	// Initiate a bulk ingest
	err := ingestor.Ingest()
	if err != nil {
		return err
	}
}
```

## Development

##### Clone the repository:

```bash
mkdir $GOPATH/src/github.com/unchartedsoftware
cd $GOPATH/src/github.com/unchartedsoftware
git clone git@github.com:unchartedsoftware/deluge.git
```

##### Install dependencies:

```bash
cd deluge
make install
```
