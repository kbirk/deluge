package progress

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/unchartedsoftware/plog"

	"github.com/unchartedsoftware/deluge/util"
)

const (
	clearLine = "\x1b[2K"
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

func print() {
	// print the current progress
	fmt.Printf("%s\rIngested %s (%d docs) at a rate of %sps (%d docs / sec), current duration: %v",
		clearLine,
		util.FormatBytes(currentBytes),
		currentDocs,
		util.FormatBytes(bytesPerSec),
		docsPerSec,
		duration())
}

func tick() {
	for {
		select {
		case <-endChan:
			// print last progress
			print()
			// stop the progress ticker
			return

		default:
			// print the current progress
			print()
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
	} else {
		bytesPerSec = currentBytes
		docsPerSec = currentDocs
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
