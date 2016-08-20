package progress

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/unchartedsoftware/plog"

	"github.com/unchartedsoftware/deluge/util"
)

var (
	startTime    time.Time
	endTime      time.Time
	currentBytes uint64
	mutex        = sync.Mutex{}
)

// StartProgress sets the internal epoch and the total bytes to track.
func StartProgress() {
	startTime = time.Now()
	currentBytes = 0
}

// EndProgress sets the end time.
func EndProgress() {
	endTime = time.Now()
}

// UpdateProgress will update and print a human readable progress message for
// a given task.
func UpdateProgress(bytes uint64) {
	mutex.Lock()
	currentBytes += bytes
	fCurrentBytes := float64(currentBytes)
	elapsedSec := time.Since(startTime).Seconds()
	bytesPerSec := 1.0
	if elapsedSec > 0 {
		bytesPerSec = fCurrentBytes / elapsedSec
	}
	fmt.Printf("\rIngested %+8s at a rate of %+8sps, current duration: %v",
		util.FormatBytes(fCurrentBytes),
		util.FormatBytes(bytesPerSec),
		time.Since(startTime))
	mutex.Unlock()
	runtime.Gosched()
}

// PrintFailure prints the total duration of the processed task.
func PrintFailure() {
	elapsed := endTime.Sub(startTime)
	fmt.Print("\n")
	log.Infof("Ingestion failed after %v", elapsed)
}

// PrintSuccess prints the total duration of the processed task.
func PrintSuccess() {
	elapsed := endTime.Sub(startTime)
	fmt.Print("\n")
	log.Infof("Ingestion completed in %v", elapsed)
}
