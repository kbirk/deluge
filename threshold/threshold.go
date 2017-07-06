package threshold

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
)

const (
	minimumToCheck = 10
)

var (
	errs    = make([]error, 0)
	mu      = sync.Mutex{}
	success uint64
)

// NewErr returns a threshold overflow error.
func NewErr(threshold float64) error {
	return fmt.Errorf("Ratio of errors to successes has surpassed threshold of `%f`",
		threshold)
}

// CheckErr checks if an error exsits and if so adds the error to the error
// checking count.
func CheckErr(err error, threshold float64) bool {
	if err == nil {
		return false
	}
	mu.Lock()
	errs = append(errs, err)
	numErrors := uint64(len(errs))
	numTotal := numErrors + success
	mu.Unlock()
	// don't fail unless until a minimum number of docs have been processed
	if numTotal < minimumToCheck {
		return false
	}
	ratio := 1.0 - (float64(atomic.LoadUint64(&success)) / float64(numErrors))
	return ratio > threshold
}

// AddSuccess adds a success to the success count.
func AddSuccess() {
	atomic.AddUint64(&success, 1)
}

// Errs returns all ingestion errors.
func Errs() []error {
	return errs
}

// SampleErrs returns an N sized sample of errors.
func SampleErrs(n int) []error {
	if len(errs) < n {
		return errs
	}
	stride := float64(len(errs)) / float64(n)
	samples := make([]error, n)
	for i := 0; i < n; i++ {
		index := int(math.Floor(float64(i) * stride))
		samples[i] = errs[index]
	}
	return samples
}
