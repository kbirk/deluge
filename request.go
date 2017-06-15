package deluge

// BulkRequest represents a bulked elasticsearch request.
type BulkRequest interface {
	Add(string, string, interface{})
	EstimatedSizeInBytes() int64
	Size() int
	Send() (uint64, error)
	Took() uint64
}
