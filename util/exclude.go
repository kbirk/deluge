package util

import (
	"os"
)

// ShouldExclude returns true if the file / dir name is to be excluded.
func ShouldExclude(name string, excludes []string) bool {
	for _, exclude := range excludes {
		if name == exclude {
			return true
		}
	}
	return false
}

// IsValidDir returns true if the directory is valid and not excluded.
func IsValidDir(file os.FileInfo, excludes []string) bool {
	return file.IsDir() &&
		!ShouldExclude(file.Name(), excludes)
}

// IsValidFile returns true if the file is valid and not excluded.
func IsValidFile(file os.FileInfo, excludes []string) bool {
	return !file.IsDir() &&
		!ShouldExclude(file.Name(), excludes) &&
		file.Size() > 0
}
