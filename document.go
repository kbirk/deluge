package deluge

// Document represents all necessary info to create an index and ingest a
// document.
type Document interface {
	SetData(interface{}) error
	GetSource() (interface{}, error)
	GetID() (string, error)
	GetMapping() (string, error)
	GetType() (string, error)
}

// Constructor represents a constructor that instantiates a new deluge document.
type Constructor func() (Document, error)
