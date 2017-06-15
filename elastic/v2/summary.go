package elastic

// IndexSummary represents a summary of an elasticsearch index.
type IndexSummary struct {
	numDocs  uint64
	byteSize uint64
}

// ByteSize returns the size of the index in bytes.
func (i *IndexSummary) ByteSize() uint64 {
	return i.byteSize
}

// NumDocs returns the number of documents in the index.
func (i *IndexSummary) NumDocs() uint64 {
	return i.byteSize
}
