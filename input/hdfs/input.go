package hdfs

import (
	"fmt"
	"io"
	"os"

	"github.com/unchartedsoftware/deluge/util"
)

// Client represents an HDFS client.
type Client interface {
	Open(string) (io.Reader, error)
	ReadDir(string) ([]os.FileInfo, error)
}

// Input represents an HDFS input type.
type Input struct {
	endpoint string
	path     string
	client   Client
	index    int
	sources  []*Source
}

// Source represents an HDFS file source.
type Source struct {
	file os.FileInfo
	path string
}

func getInfo(client Client, path string, excludes []string) ([]*Source, error) {
	// get info on path
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	// data to populate
	var sources []*Source
	// check if dir
	if !info.IsDir() {
		// is file
		// add source
		sources = append(sources, &Source{
			file: info,
			path: path,
		})
	} else {
		// is directory
		// read target files
		files, err := client.ReadDir(path)
		if err != nil {
			return nil, err
		}
		// for each file / dir
		for _, file := range files {
			if util.IsValidDir(file, excludes) {
				// depth-first traversal into sub directories
				children, err := getInfo(client, path+"/"+file.Name(), excludes)
				if err != nil {
					return nil, err
				}
				sources = append(sources, children...)
			} else if util.IsValidFile(file, excludes) {
				// add source
				sources = append(sources, &Source{
					file: file,
					path: path,
				})
			}
		}
	}
	// return ingest info
	return sources, nil
}

// NewInput instantiates a new instance of a file input.
func NewInput(client Client, path string, excludes []string) (*Input, error) {
	sources, err := getInfo(client, path, excludes)
	if err != nil {
		return nil, err
	}
	return &Input{
		path:    path,
		client:  client,
		sources: sources,
		index:   0,
	}, nil
}

// Next opens the file and returns the reader.
func (i *Input) Next() (io.Reader, error) {
	if i.index > len(i.sources)-1 {
		return nil, io.EOF
	}
	source := i.sources[i.index]
	reader, err := i.client.Open(source.path + "/" + source.file.Name())
	if err != nil {
		return nil, err
	}
	i.index++
	return reader, nil
}

// Summary returns a string containing summary information.
func (i *Input) Summary() string {
	totalBytes := int64(0)
	for _, source := range i.sources {
		totalBytes += source.file.Size()
	}
	return fmt.Sprintf("Input `%s` contains %d files containing %s",
		i.path,
		len(i.sources),
		util.FormatBytes(totalBytes))
}
