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

// SetUser sets the HDFS user.
func SetUser(user string) ClientOptionFunc {
	return func(c *Client) error {
		if user != "" {
			c.user = user
		}
		return nil
	}
}

// Client represents an HDFS client.
type Client struct {
	endpoint string
	user     string
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
	var conn *hdfs.Client
	var err error
	if client.user != "" {
		conn, err = hdfs.NewForUser(client.endpoint, client.user)
	} else {
		conn, err = hdfs.New(client.endpoint)
	}
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

// Close closes the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// ReadDir reads the directory named by dirname and returns a list of sorted directory entries.
func (c *Client) ReadDir(dirname string) ([]os.FileInfo, error) {
	return c.conn.ReadDir(dirname)
}

// Stat returns an os.FileInfo describing the named file or directory.
func (c *Client) Stat(path string) (os.FileInfo, error) {
	return c.conn.Stat(path)
}
