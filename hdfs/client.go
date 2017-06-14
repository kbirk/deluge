package hdfs

import (
	"io"
	"os"

	"github.com/colinmarc/hdfs"
)

// ClientOptionFunc is a function that configures an HDFS client.
type ClientOptionFunc func(*Client) error

// SetURL sets the HDFS endpoint URL.
func SetURL(url string) ClientOptionFunc {
	return func(c *Client) error {
		if url != "" {
			c.endpoint = url
		}
		return nil
	}
}

// Client represents an HDFS client.
type Client struct {
	endpoint string
	conn     *hdfs.Client
}

// NewClient instantiates and returns a new HDFS client.
func NewClient(options ...ClientOptionFunc) (*Client, error) {
	// instantiate client
	client := &Client{
		endpoint: "localhost:8020",
	}
	// run the options through it
	for _, option := range options {
		if err := option(client); err != nil {
			return nil, err
		}
	}
	// create underlying connection
	conn, err := hdfs.New(client.endpoint)
	if err != nil {
		return nil, err
	}
	client.conn = conn
	return client, nil
}

// Open returns an io.Reader which can be used for reading.
func (c *Client) Open(path string) (io.Reader, error) {
	return c.conn.Open(path)
}

// ReadDir reads the directory named by dirname and returns a list of sorted directory entries.
func (c *Client) ReadDir(dirname string) ([]os.FileInfo, error) {
	return c.conn.ReadDir(dirname)
}
