# Deluge

[![Godoc](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](http://godoc.org/github.com/unchartedsoftware/deluge)
[![Build Status](https://travis-ci.org/unchartedsoftware/deluge.svg?branch=master)](https://travis-ci.org/unchartedsoftware/deluge)
[![Go Report Card](https://goreportcard.com/badge/github.com/unchartedsoftware/deluge)](https://goreportcard.com/report/github.com/unchartedsoftware/deluge)

> Fast and sustainable Elasticsearch ingestion, migration, and cloning.

## Usage

This package provides facilities for customizable bulk ingests of data into [Elasticsearch](https://github.com/elastic/elasticsearch).

## Features

- Concurrent input loading and parsing via goroutine worker pool
- Sustainable long-term ingestion via fixed-size connection pool and back-pressure
- Configurable error thresholding to prevent hard crashes on sporadic parsing errors
- Clean, simple, and highly extensible interfaces for customizable ingests

## Dependencies

Requires the [Go](https://golang.org/) programming language binaries with the `GOPATH` environment variable specified and `$GOPATH/bin` in your `PATH`.

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

NOTE: Requires [Glide](https://glide.sh) along with [Go](https://golang.org/) version 1.6+.

## Example

##### Implement the `elastic.Document` interface:

```go
package sample

import (
	"fmt"

	"github.com/unchartedsoftware/deluge"
	"github.com/unchartedsoftware/deluge/document"
)

// Document overrides the CSV document type.
type Document struct {
	document.CSV
}

// NewDocument instantiates and returns a new document.
func NewDocument() (deluge.Document, error) {
	return &Document{}, nil
}

// GetID returns the document's id.
func (d *Document) GetID() (string, error) {
	id, ok := d.String(0)
	if !ok {
		return "", fmt.Errorf("no id found")
	}
	return id, nil
}

// GetType returns the document's type.
func (d *Document) GetType() (string, error) {
	return "datum", nil
}

// GetMapping returns the document's mapping.
func (d *Document) GetMapping() (string, error) {
	return `{
		"datum": {
			"properties":{
				"description": {
					"type": "string"
				}
			}
		}
	}`, nil
}

// GetSource returns the source portion of the document.
func (d *Document) GetSource() (interface{}, error) {
	desc, ok := d.String(1)
	if !ok {
		return nil, fmt.Errorf("no description found")
	}
	return map[string]interface{}{
		"description": desc,
	}, nil
}
```

##### Use the `deluge.Ingestor` to bulk ingest:

```go
package main

import (
	"runtime"

	"github.com/unchartedsoftware/deluge"
	"github.com/unchartedsoftware/deluge/elastic/v2"
	"github.com/unchartedsoftware/deluge/input/file"

	"github.com/username/example/sample"
)

func main() {

	// Use all CPUs
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Create a filesystem input type
	input := file.NewInput(
		"/path/to/data",
		[ "files", "or", "dirs", "to", "exclude" ])

	// Create the elasticsearch client
	client, err := elastic.NewClient(
		elastic.SetURL("localhost:9200"),
		elastic.SetMaxRetries(10),
		elastic.SetSniff(false),
		elastic.SetGzip(true))
	if err != nil {
		log.Fatal(err)
	}

	// Create the ingestor object
	ingestor, err := deluge.NewIngestor(
		deluge.SetDocument(sample.NewDocument)
		deluge.SetInput(input),
		deluge.SetClient(client),
		deluge.SetIndex("test_index"),
		deluge.SetErrorThreshold(0.05),
		deluge.SetNumWorkers(8),
		deluge.SetActiveConnections(16),
		deluge.SetCompression("gzip"),
		deluge.SetBulkByteSize(1024*1024*20),
		deluge.SetScanBufferSize(1024*1024*2),
		deluge.ClearExistingIndex(),
		deluge.SetNumReplicas(1)),
	if err != nil {
		log.Fatal(err)
	}

	// Initiate a bulk ingest job
	err = ingestor.Ingest()
	if err != nil {
		// critical error or error ratio above threshold
		log.Fatal(err)
	}

	// Check for any errors
	errs := deluge.DocErrs()
	if len(errs) > 0 {
		// sample 10 errors
		for _, err := range deluge.SampleDocErrs(10) {
			log.Print(err)
		}
	}
}
```

## Development

##### Clone the repository:

```bash
mkdir -p $GOPATH/src/github.com/unchartedsoftware
cd $GOPATH/src/github.com/unchartedsoftware
git clone git@github.com:unchartedsoftware/deluge.git
```

##### Install dependencies:

```bash
cd deluge
make install
```
