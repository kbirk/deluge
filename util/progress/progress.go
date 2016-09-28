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
	mutex        = sync.Mutex{}
	endChan      chan bool
)

func tick() {
	for {
		select {
		case <-endChan:
			// stop the progress ticker
			return

		default:
			// print the current progress
			now := time.Now().Round(time.Second)
			duration := now.Sub(startTime)
			elapsedSec := int64(duration.Seconds())
			bytesPerSec := int64(1)
			if elapsedSec > 0 {
				bytesPerSec = currentBytes / elapsedSec
			}
			fmt.Printf("\rIngested %+8s at a rate of %+8sps, current duration: %+9v",
				util.FormatBytes(currentBytes),
				util.FormatBytes(bytesPerSec),
				duration)
			// sleep for a second
			time.Sleep(time.Second)
		}
	}
}

// StartProgress sets the internal epoch and the total bytes to track.
func StartProgress() {
	startTime = time.Now().Round(time.Second)
	currentBytes = 0
	endChan = make(chan bool)
	go tick()
}

// EndProgress sets the end time.
func EndProgress() {
	endTime = time.Now().Round(time.Second)
	endChan <- true
	close(endChan)
}

// UpdateProgress will update and print a human readable progress message for
// a given task.
func UpdateProgress(bytes int64) {
	mutex.Lock()
	currentBytes += bytes
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
