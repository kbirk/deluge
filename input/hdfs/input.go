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
	Stat(string) (os.FileInfo, error)
}

// Input represents an HDFS input type.
type Input struct {
	endpoint string
	client   Client
	paths    []string
	sources  []*Source
	index    int
}

// Source represents an HDFS file source.
type Source struct {
	file     os.FileInfo
	fullpath string
}

func getInfo(client Client, path string, excludes []string) ([]*Source, error) {
	// get info on path
	info, err := client.Stat(path)
	if err != nil {
		return nil, err
	}
	// data to populate
	var sources []*Source
	// check if dir
	if util.IsValidFile(info, excludes) {
		// is file
		sources = append(sources, &Source{
			file:     info,
			fullpath: path,
		})
	}
	if util.IsValidDir(info, excludes) {
		// is directory
		infos, err := client.ReadDir(path)
		if err != nil {
			return nil, err
		}
		// for each file / dir
		for _, info := range infos {
			// get full path
			fullpath := path + "/" + info.Name()
			// depth-first traversal into sub directories
			children, err := getInfo(client, fullpath, excludes)
			if err != nil {
				return nil, err
			}
			sources = append(sources, children...)
		}
	}
	// return ingest info
	return sources, nil
}

// NewInput instantiates a new instance of a file input.
func NewInput(client Client, paths []string, excludes []string) (*Input, error) {
	var sources []*Source
	for _, path := range paths {
		srcs, err := getInfo(client, path, excludes)
		if err != nil {
			return nil, err
		}
		sources = append(sources, srcs...)
	}
	return &Input{
		paths:   paths,
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
	reader, err := i.client.Open(source.fullpath)
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
	return fmt.Sprintf("Input %v contains %d files containing %s",
		i.paths,
		len(i.sources),
		util.FormatBytes(totalBytes))
}
