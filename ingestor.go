package deluge

import (
	"bufio"
	"compress/bzip2"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"
	"sync"

	log "github.com/unchartedsoftware/plog"

	"github.com/unchartedsoftware/deluge/equalizer"
	"github.com/unchartedsoftware/deluge/pool"
	"github.com/unchartedsoftware/deluge/progress"
	"github.com/unchartedsoftware/deluge/threshold"
)

const (
	defaultHost                 = "localhost"
	defaultPort                 = "9200"
	defaultClearExisting        = true
	defaultNumActiveConnections = 8
	defaultNumWorkers           = 8
	defaultCompression          = ""
	defaultNumReplicas          = 1
	defaultThreshold            = 0.01
	defaultBulkByteSize         = 1024 * 1024 * 20
	defaultScanBufferSize       = 1024 * 1024 * 2
	defaultUpdateMapping        = false
	defaultReadOnly             = false
	defaultBlockWrite           = false
)

// Ingestor is an Elasticsearch ingestor client. Create one by calling
// NewIngestor.
type Ingestor struct {
	input                Input
	documentCtor         Constructor
	index                string
	client               Client
	clearExisting        bool
	numActiveConnections int
	numWorkers           int
	numReplicas          int
	compression          string
	threshold            float64
	bulkByteSize         int64
	scanBufferSize       int
	updateMapping        bool
	readOnly             bool
	blockWrite           bool
	bulkSizeOptimiser    Optimiser
	mutex                *sync.RWMutex
	callbackWG           *sync.WaitGroup
}

// NewIngestor instantiates and configures a new Ingestor instance.
func NewIngestor(options ...IngestorOptionFunc) (*Ingestor, error) {
	// instantiate the ingestor
	ingestor := &Ingestor{
		clearExisting:        defaultClearExisting,
		compression:          defaultCompression,
		numActiveConnections: defaultNumActiveConnections,
		numWorkers:           defaultNumWorkers,
		numReplicas:          defaultNumReplicas,
		threshold:            defaultThreshold,
		bulkByteSize:         defaultBulkByteSize,
		scanBufferSize:       defaultScanBufferSize,
		updateMapping:        defaultUpdateMapping,
		readOnly:             defaultReadOnly,
		blockWrite:           defaultBlockWrite,
		mutex:                &sync.RWMutex{},
		callbackWG:           &sync.WaitGroup{},
	}
	// run the options through it
	for _, option := range options {
		if err := option(ingestor); err != nil {
			return nil, err
		}
	}
	return ingestor, nil
}

func (i *Ingestor) prepareIndex() error {
	// check if index exists
	indexExists, err := i.client.IndexExists(i.index)
	if err != nil {
		return err
	}
	// if index exists
	if indexExists && i.clearExisting {
		// send the delete index request
		log.Infof("Deleting existing index `%s`", i.index)
		err := i.client.DeleteIndex(i.index)
		if err != nil {
			return fmt.Errorf("Error occurred while deleting index: %v", err)
		}
	}
	// instantiate a new document
	document, err := i.documentCtor()
	if err != nil {
		return err
	}
	// get the document mapping
	mapping, err := document.GetMapping()
	if err != nil {
		return err
	}
	// get the document type name
	typ, err := document.GetType()
	if err != nil {
		return err
	}
	// if index does not exist at this point, create it
	if !indexExists || i.clearExisting {
		// send create index request
		log.Infof("Creating index `%s`", i.index)
		err := i.client.CreateIndex(i.index, mapping)
		if err != nil {
			return fmt.Errorf("Error occurred while creating index: %v", err)
		}
	} else if i.updateMapping {
		// send put mapping request
		log.Infof("Putting mapping `%s`", i.index)
		err := i.client.PutMapping(i.index, typ, mapping)
		if err != nil {
			return fmt.Errorf("Error occurred while updating mapping for index: %v", err)
		}
	}
	return nil
}

func (i *Ingestor) enableReplicas() error {
	log.Infof("Enabling replicas for index `%s`", i.index)
	err := i.client.EnableReplicas(i.index, i.numReplicas)
	if err != nil {
		return fmt.Errorf("Error occurred while enabling replicas: %v", err)
	}
	return nil
}

func (i *Ingestor) getBulkByteSize() int64 {
	i.mutex.RLock()
	bytes := i.bulkByteSize
	i.mutex.RUnlock()

	return bytes
}

func (i *Ingestor) setBulkByteSize(bulkByteSize int64) {
	i.mutex.Lock()
	i.bulkByteSize = bulkByteSize
	i.mutex.Unlock()
}

// Ingest will run the ingest job.
func (i *Ingestor) Ingest() error {

	// check that we have the required options set
	if i.index == "" {
		return fmt.Errorf("Ingestor index has not been set with SetIndex() option")
	}
	if i.documentCtor == nil {
		return fmt.Errorf("Ingestor document constructor has not been set with SetDocument() option")
	}
	if i.input == nil {
		return fmt.Errorf("Ingestor input has not been set with SetInput() option")
	}
	if i.client == nil {
		return fmt.Errorf("Ingestor Elasticsearch client has not been set with SetClient() option")
	}

	// print input summary
	log.Info(i.input.Summary())

	// prepare elasticsearch index
	err := i.prepareIndex()
	if err != nil {
		return err
	}

	// open the backpressure equalizer
	equalizer.Open(i.numActiveConnections)

	// create pool of size N
	p := pool.New(i.numWorkers)

	// start progress tracking
	progress.StartProgress()

	// start optimising if signalled.
	if i.bulkSizeOptimiser != nil {
		s := NewBulkSize(i)
		go i.bulkSizeOptimiser.Optimise(s)
	}

	// launch the ingest job
	err = p.Execute(i.newlineWorker(), i.input)
	if err != nil {
		// error threshold was surpassed or there was a fatal error
		progress.EndProgress()
		progress.PrintFailure()
		return err
	}

	// wait until all callbacks executed
	i.callbackWG.Wait()

	// success
	progress.EndProgress()
	progress.PrintSuccess()

	// close the backpressure equalizer
	equalizer.Close()

	// enable replication
	if i.numReplicas > 0 {
		err := i.enableReplicas()
		if err != nil {
			return err
		}
	}

	// set the index as read-only (if necessary)
	if err := i.client.SetReadOnly(i.index, i.readOnly); err != nil {
		return err
	}

	// set the index as block write (if necessary)
	if err := i.client.SetBlockWrite(i.index, i.blockWrite); err != nil {
		return err
	}

	return nil
}

// DocErrs returns all document ingest errors.
func DocErrs() []error {
	return threshold.Errs()
}

// SampleDocErrs returns an N sized sample of document ingest errors.
func SampleDocErrs(n int) []error {
	return threshold.SampleErrs(n)
}

func getReader(reader io.Reader, compression string) (io.Reader, error) {
	// use compression based reader if specified
	switch compression {
	case "gzip":
		return gzip.NewReader(reader)
	case "bzip2":
		return bzip2.NewReader(reader), nil
	case "flate":
		return flate.NewReader(reader), nil
	case "zlib":
		return zlib.NewReader(reader)
	default:
		return reader, nil
	}
}

func (i *Ingestor) createProgressCallback(bytes, docs int64) equalizer.CallbackFunc {
	// increment callback waitgroup
	i.callbackWG.Add(1)
	return func(err error) {
		if err == nil {
			// update and print current progress if no error
			progress.UpdateProgress(bytes, docs)
		}
		// decrement waitgroup
		i.callbackWG.Done()
	}
}

func (i *Ingestor) addLineToBulkRequest(bulk BulkRequest, line string) (bool, error) {
	// instantiate a new document
	document, err := i.documentCtor()
	if err != nil {
		return false, err
	}
	// set data for document
	err = document.SetData(line)
	if err != nil {
		return false, err
	}
	// get id from document
	id, err := document.GetID()
	if err != nil {
		return false, err
	}
	// gracefully handle nil id
	if id == "" {
		return false, nil
	}
	// get type from document
	typ, err := document.GetType()
	if err != nil {
		return false, err
	}
	// gracefully handle nil type
	if typ == "" {
		return false, nil
	}
	// get source from document
	source, err := document.GetSource()
	if err != nil {
		return false, err
	}
	// gracefully handle nil source
	if source == nil {
		return false, nil
	}
	// add document to bulk req
	bulk.Add(typ, id, source)
	// flag that the line was parsed successfully
	return true, nil
}

func (i *Ingestor) newlineWorker() pool.Worker {
	return func(next io.Reader) error {

		// get decompress reader (if compression is specified / supported)
		reader, err := getReader(next, i.compression)
		if threshold.CheckErr(err, i.threshold) {
			return threshold.NewErr(i.threshold)
		}

		// scan file line by line
		scanner := bufio.NewScanner(reader)
		// allocate a large enough buffer
		scanner.Buffer(make([]byte, i.scanBufferSize), i.scanBufferSize)

		for {
			// total bytes sent
			bytes := int64(0)
			docs := int64(0)

			// create a new bulk request object
			bulk := i.client.NewBulkRequest(i.index)

			// begin reading file, line by line
			for scanner.Scan() {

				// read line of file
				line := scanner.Text()

				// add line to bulk index request
				success, err := i.addLineToBulkRequest(bulk, line)
				if threshold.CheckErr(err, i.threshold) {
					return threshold.NewErr(i.threshold)
				}

				// ensure that the request was created
				if success {
					docs = docs + 1

					// flag this document as successful
					threshold.AddSuccess()
					// check if we have hit batch size limit
					if bulk.EstimatedSizeInBytes() >= i.getBulkByteSize() {
						// ready to send
						break
					}
				}
			}

			// check if scanner encountered an err
			err := scanner.Err()
			if threshold.CheckErr(err, i.threshold) {
				return threshold.NewErr(i.threshold)
			}

			// if no actions, we are finished
			if bulk.Size() == 0 {
				break
			}

			// add total bytes
			bytes += int64(bulk.EstimatedSizeInBytes())

			// create the callback to be executed after this bulk request
			// succeeds. This is required ensure that the correct `bytes`
			// value is snapshotted.
			callback := i.createProgressCallback(bytes, docs)

			// send the request through the equalizer, this will wait until the
			// equalizer determines ES is 'ready'.
			// NOTE: Due to the asynchronous nature of the equalizer, error
			// values returned here may not be caused from this worker
			// goroutine.
			err = equalizer.Send(bulk, callback)
			if err != nil {
				// always return on bulk ingest error
				return err
			}

		}
		return nil
	}
}
