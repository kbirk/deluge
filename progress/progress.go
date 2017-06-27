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
	currentBytes int64
	currentDocs  int64
	bytesPerSec  = int64(1)
	docsPerSec   = int64(1)
	mutex        = sync.Mutex{}
	endChan      chan bool
)

func duration() time.Duration {
	now := time.Now().Round(time.Second)
	return now.Sub(startTime)
}

func tick() {
	for {
		select {
		case <-endChan:
			// stop the progress ticker
			return

		default:
			// print the current progress
			fmt.Printf("\rIngested %+9s (%d docs) at a rate of %+8sps (%d docs / sec), current duration: %+9v",
				util.FormatBytes(currentBytes),
				currentDocs,
				util.FormatBytes(bytesPerSec),
				docsPerSec,
				duration())
			// sleep for a second
			time.Sleep(time.Second)
		}
	}
}

// StartProgress sets the internal epoch and the total bytes to track.
func StartProgress() {
	startTime = time.Now().Round(time.Second)
	currentBytes = 0
	currentDocs = 0
	endChan = make(chan bool)
	go tick()
}

// EndProgress sets the end time.
func EndProgress() {
	endTime = time.Now().Round(time.Second)
	endChan <- true
	close(endChan)
}

// GetDocCount returns the current doc count.
func GetDocCount() int64 {
	mutex.Lock()
	docs := currentDocs
	mutex.Unlock()

	return docs
}

// UpdateProgress will update and print a human readable progress message for
// a given task.
func UpdateProgress(bytes, docs int64) {
	mutex.Lock()
	// increment the totals
	currentBytes += bytes
	currentDocs += docs

	// set the current ingest speed
	elapsedSec := int64(duration().Seconds())
	if elapsedSec > 0 {
		bytesPerSec = currentBytes / elapsedSec
		docsPerSec = currentDocs / elapsedSec
	}

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
