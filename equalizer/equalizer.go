package equalizer

import (
	"sync"
	"time"
)

const (
	// number of ingestion durations to use when calculating the avg
	maxNumRates = 64
)

// Equalizer represents an equalzier to apply backpressure to bulk requests.
var (
	ready          chan error
	waitGroup      *sync.WaitGroup
	rates          []uint64
	maxNumRequests int
)

// CallbackFunc represents an simple callback function to be executed after a
// successful send.
type CallbackFunc func()

// Open initiializes the equalizer and readies it for sending requests.
func Open(size int) {
	// get max number of requests
	maxNumRequests = size
	// initialize channels
	ready = make(chan error)
	waitGroup = new(sync.WaitGroup)
	go func() {
		waitGroup.Add(1)
		// send as many ready messages as there are concurrent requests
		for i := 0; i < maxNumRequests; i++ {
			ready <- nil
		}
	}()
}

func getAvg() float64 {
	total := uint64(0)
	for _, ms := range rates {
		total += ms
	}
	return float64(total) / float64(len(rates))
}

func measure(ms uint64) {
	rates = append(rates, ms)
	if len(rates) > maxNumRates {
		// if past max rates, pop oldest one off
		rates = rates[1:]
	}
}

func throttle(took uint64) {
	// get difference between the time it took to generate the payload vs
	// the time it takes ES to ingest
	delta := getAvg() - float64(took)
	// wait the difference if it is positive
	if delta > 0 {
		time.Sleep(time.Millisecond * time.Duration(delta))
	}
}

func forwardRequest(req *Request, fn CallbackFunc) {
	throttle(req.took)
	took, err := req.Send()
	if fn != nil && err == nil {
		// only execute callback on success
		fn()
	}
	measure(took)
	ready <- err
	waitGroup.Done()
}

// Send dispatches a request through the equalizer. This call will wait on
// pending requests, and if said pending requests results in an error, will
// return it.
func Send(req *Request, fn CallbackFunc) error {
	req.stamp()
	err := <-ready
	if err != nil {
		return err
	}
	waitGroup.Add(1)
	go forwardRequest(req, fn)
	return nil
}

// Close disables the equalizer so that it no longer listens to any incoming bulk requests.
func Close() {
	// at this point any requests will be blocked waiting for the eq to read
	// from the ready channel, so lets grab all these right now so the Equalizer
	// can close
	go func() {
		for i := 0; i < maxNumRequests; i++ {
			<-ready
		}
		waitGroup.Done()
	}()
	// ensure there are no pending responses
	waitGroup.Wait()
	// safe to close ready channel now
	close(ready)
}
