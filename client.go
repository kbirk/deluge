package deluge

// Client represents the elasticsearch client interface.
type Client interface {
	NewBulkRequest(string) BulkRequest
	IndexExists(string) (bool, error)
	DeleteIndex(string) error
	CreateIndex(string, string) error
	PutMapping(string, string, string) error
	EnableReplicas(string, int) error
	SetReadOnly(string, bool) error
}
