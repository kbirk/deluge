package deluge

// Document represents all necessary info to create an index and ingest a document.
type Document interface {
	SetData(interface{}) error
	GetSource() (interface{}, error)
	GetID() (string, error)
	GetMapping() (string, error)
	GetType() string
}
