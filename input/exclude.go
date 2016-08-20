package input

// ShouldExclude returns true if the file / dir name is to be excluded.
func ShouldExclude(name string, excludes []string) bool {
	for _, exclude := range excludes {
		if name == exclude {
			return true
		}
	}
	return false
}
