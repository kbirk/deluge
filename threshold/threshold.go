package threshold

import (
	"sync"
	"sync/atomic"
)

var (
	errs    = make([]error, 0)
	mu      = sync.Mutex{}
	success uint64
)

// CheckErr checks if an error exsits and if so adds the error to the error
// checking count.
func CheckErr(err error, threshold float64) bool {
	if err == nil {
		return false
	}
	mu.Lock()
	errs = append(errs, err)
	numErrors := uint64(len(errs))
	mu.Unlock()
	ratio := 1.0 - (float64(atomic.LoadUint64(&success)) / float64(numErrors))
	return ratio > threshold
}

// AddSuccess adds a success to the success count.
func AddSuccess() {
	atomic.AddUint64(&success, 1)
}

// Errs returns all ingestion errors.
func Errs() []error {
	return errs[0:]
}
