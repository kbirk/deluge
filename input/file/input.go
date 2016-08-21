package file

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/unchartedsoftware/deluge"
	"github.com/unchartedsoftware/deluge/input"
	"github.com/unchartedsoftware/deluge/util"
)

// Input represents an input type for reading files off a filesystem.
type Input struct {
	path    string
	index   int
	sources []*Source
}

// Source represents a filesystem file source.
type Source struct {
	file os.FileInfo
	path string
}

func getInfo(path string, excludes []string) ([]*Source, error) {
	// read target files
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	// data to populate
	var sources []*Source
	// for each file / dir
	for _, file := range files {
		if input.IsValidDir(file, excludes) {
			// depth-first traversal into sub directories
			children, err := getInfo(path+"/"+file.Name(), excludes)
			if err != nil {
				return nil, err
			}
			sources = append(sources, children...)
		} else if input.IsValidFile(file, excludes) {
			// add source
			sources = append(sources, &Source{
				file: file,
				path: path,
			})
		}
	}
	// return ingest info
	return sources[0:], nil
}

// NewInput instantiates a new instance of a file input.
func NewInput(path string, excludes []string) (deluge.Input, error) {
	sources, err := getInfo(path, excludes)
	if err != nil {
		return nil, err
	}
	return &Input{
		path:    path,
		sources: sources,
		index:   0,
	}, nil
}

// Next opens the file and returns the reader.
func (i *Input) Next() (io.Reader, error) {
	if i.index > len(i.sources)-1 {
		return nil, input.ErrEOS
	}
	source := i.sources[i.index]
	reader, err := os.Open(source.path + "/" + source.file.Name())
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
