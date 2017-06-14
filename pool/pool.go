package pool

import (
	"io"
)

// Work represents work that is accepted by a worker.
type Work interface {
	Next() (io.Reader, error)
}

// Worker represents a designated worker function to batch in a pool.
type Worker func(io.Reader) error

// Pool represents a single goroutine pool for batching workers.
type Pool struct {
	WorkChan chan io.Reader
	ErrChan  chan error
	KillChan chan bool
	Size     int
}

// New returns a new pool object with the given worker size
func New(size int) *Pool {
	return &Pool{
		WorkChan: make(chan io.Reader),
		ErrChan:  make(chan error),
		KillChan: make(chan bool),
		Size:     size,
	}
}

func (p *Pool) dispatchWorker(worker Worker) {
	// tell the pool that this worker is ready
	p.ErrChan <- nil
	// begin worker loop
	for {
		select {
		case work := <-p.WorkChan:
			// do work
			err := worker(work)
			// broadcast work response to pool, if nil worker is ready for more
			// work, if not, then shut down the pool
			p.ErrChan <- err

		case <-p.KillChan:
			// kill worker
			return
		}
	}
}

func (p *Pool) close(closingErr error) error {
	// workers will currently be blocked trying to send a ready/error message
	// to the pool. We need to absorb those messages now so that they will be
	// able to process the kill signals.
	go func() {
		for i := 0; i < p.Size; i++ {
			err := <-p.ErrChan
			// still need to check for errors
			if err != nil && closingErr == nil {
				closingErr = err
			}
		}
	}()
	// send a kill message to all workers
	for i := 0; i < p.Size; i++ {
		p.KillChan <- true
	}
	// workers are all dead now, it is safe to close the channels
	close(p.WorkChan)
	close(p.KillChan)
	close(p.ErrChan)
	// return the first error
	return closingErr
}

func (p *Pool) open(worker Worker) {
	// for each worker in pool
	for i := 0; i < p.Size; i++ {
		// dispatch the workers, they will wait until the input channel is closed
		go p.dispatchWorker(worker)
	}
}

// Execute launches a batch of ingest workers with the provided ingest information.
func (p *Pool) Execute(worker Worker, work Work) error {
	// open the pool and dispatch the workers
	p.open(worker)
	// process all files by spreading them to free workers, this blocks until
	// a worker is available, or exits if there is an error
	for {
		err := <-p.ErrChan
		if err != nil {
			// error has occurred, close the pool and return error
			return p.close(err)
		}
		next, err := work.Next()
		if err == io.EOF {
			// end of input stream, we are done
			break
		}
		if err != nil {
			// error has occurred, close the pool and return error
			return p.close(err)
		}
		// continue passing files to workers
		p.WorkChan <- next
	}
	// when work is done safely close the pool
	return p.close(nil)
}
