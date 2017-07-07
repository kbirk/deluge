package kafka

import (
	"bytes"
	"io"
)

// Client represents an HDFS client.
type Client interface {
	Consume() ([]byte, error)
}

// Input represents an input type for scanning documents out of kafka.
type Input struct {
	client Client
}

// NewInput instantiates a new instance of an kafka input.
func NewInput(client Client) (*Input, error) {
	return &Input{
		client: client,
	}, nil
}

// Next returns the io.Reader to scan the index for more docs.
func (i *Input) Next() (io.Reader, error) {
	msg, err := i.client.Consume()
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(msg), nil
}

// Summary returns a string containing summary information.
func (i *Input) Summary() string {
	return ""
}
