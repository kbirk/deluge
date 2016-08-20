package file

import (
	"io/ioutil"
	"os"

	"github.com/unchartedsoftware/plog"

	"github.com/unchartedsoftware/deluge/input"
	"github.com/unchartedsoftware/deluge/util"
)

// Input represents an input type for reading files off a filesystem.
type Input struct {
	index    int
	info     *input.Info
	path     string
	excludes []string
}

// NewInput instantiates a new instance of a file input.
func NewInput(path string, excludes []string) (input.Input, error) {
	return &Input{
		path:     path,
		excludes: excludes,
	}, nil
}

func isValidDir(file os.FileInfo, excludes []string) bool {
	return file.IsDir() && !input.ShouldExclude(file.Name(), excludes)
}

func isValidFile(file os.FileInfo, excludes []string) bool {
	return !file.IsDir() &&
		!input.ShouldExclude(file.Name(), excludes) &&
		file.Name() != "_SUCCESS" &&
		file.Size() > 0
}

// Next opens the file and returns the reader.
func (i *Input) Next() (interface{}, error) {
	if i.index > len(i.info.Sources)-1 {
		return nil, input.ErrEOS
	}
	source := i.info.Sources[i.index]
	reader, err := os.Open(source.Path + "/" + source.Name)
	if err != nil {
		return nil, err
	}
	i.index++
	return reader, nil
}

// GetPath returns the base path for the input.
func (i *Input) GetPath() string {
	return i.path
}

// GetInfo returns an object containing a summation of the ingest information.
func (i *Input) GetInfo(path string) (*input.Info, error) {
	// read target files
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	// data to populate
	var sources []*input.Source
	numTotalBytes := uint64(0)
	log.Infof("Retrieving ingest info from: %s", path)
	// for each file / dir
	for _, file := range files {
		if isValidDir(file, i.excludes) {
			// depth-first traversal into sub directories
			subInfo, err := i.GetInfo(path + "/" + file.Name())
			if err != nil {
				return nil, err
			}
			sources = append(sources, subInfo.Sources...)
			numTotalBytes += subInfo.NumTotalBytes
		} else if isValidFile(file, i.excludes) {
			// add to total bytes
			numTotalBytes += uint64(file.Size())
			// store file info
			sources = append(sources, &input.Source{
				Name: file.Name(),
				Path: path,
				Size: uint64(file.Size()),
			})
		}
	}
	// print if we have found files
	if len(sources) > 0 {
		log.Infof("Found %d files containing %s of ingestible data",
			len(sources),
			util.FormatBytes(float64(numTotalBytes)))
	}
	i.info = &input.Info{
		Sources:       sources[0:],
		NumTotalBytes: numTotalBytes,
	}
	// return ingest info
	return i.info, nil
}
