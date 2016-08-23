package elastic

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/unchartedsoftware/plog"
	"gopkg.in/olivere/elastic.v3"
)

// GetBulkRequest creates and returns a pointer to a new elastic.BulkService
// for building a bulk request.
func GetBulkRequest(host string, port string, index string) (*elastic.BulkService, error) {
	client, err := getClient(host, port)
	if err != nil {
		return nil, err
	}
	return client.Bulk().
		Index(index), nil
}

// NewBulkIndexRequest creates and returns a pointer to a BulkIndexRequest object.
func NewBulkIndexRequest() *elastic.BulkIndexRequest {
	return elastic.NewBulkIndexRequest()
}

// IndexExists returns whether or not the provided index exists in elasticsearch.
func IndexExists(host string, port string, index string) (bool, error) {
	client, err := getClient(host, port)
	if err != nil {
		return false, err
	}
	return client.IndexExists(index).Do()
}

// DeleteIndex deletes the provided index in elasticsearch.
func DeleteIndex(host string, port string, index string) error {
	client, err := getClient(host, port)
	if err != nil {
		return err
	}
	res, err := client.DeleteIndex(index).Do()
	if err != nil {
		return fmt.Errorf("Error occured while deleting index: %v", err)
	}
	if !res.Acknowledged {
		return errors.New("Delete index request not acknowledged")
	}
	return nil
}

// CreateIndex creates the provided index in elasticsearch.
func CreateIndex(host string, port string, index string, body string) error {
	client, err := getClient(host, port)
	if err != nil {
		return err
	}
	res, err := client.CreateIndex(index).Body(body).Do()
	if err != nil {
		return fmt.Errorf("Error occured while creating index: %v", err)
	}
	if !res.Acknowledged {
		return errors.New("Create index request not acknowledged")
	}
	return nil
}

// EnableReplicas sets the number of replicas to 1 for a given index.
func EnableReplicas(host string, port string, index string, numReplicas int) error {
	client, err := getClient(host, port)
	if err != nil {
		return err
	}
	body := fmt.Sprintf("{\"index\":{\"number_of_replicas\":%d}}", numReplicas)
	log.Info("Enabling replicas on index '" + index + "'")
	res, err := client.IndexPutSettings(index).BodyString(body).Do()
	if err != nil {
		return fmt.Errorf("Error occured while enabling replicas: %v", err)
	}
	if !res.Acknowledged {
		return errors.New("Enable replication index request not acknowledged")
	}
	return nil
}

// PrepareIndex will ensure the provided index exists, and will optionally clear it.
func PrepareIndex(host string, port string, index string, mappings string, clearExisting bool) error {
	// check if index exists
	indexExists, err := IndexExists(host, port, index)
	if err != nil {
		return err
	}
	// if index exists
	if indexExists && clearExisting {
		log.Infof("Deleting index `%s:%s/%s`", host, port, index)
		err = DeleteIndex(host, port, index)
		if err != nil {
			return err
		}
	}
	// if index does not exist at this point, create it
	if !indexExists || clearExisting {
		log.Infof("Creating index `%s:%s/%s`", host, port, index)
		err = CreateIndex(host, port, index, `{
			"mappings":`+mappings+`,
			"settings": {
				"number_of_replicas": 0
			}
		}`)
		if err != nil {
			return err
		}
	}
	return nil
}

// IndexStats returns an index stats response.
func IndexStats(host string, port string, index string) (*elastic.IndicesStatsResponse, error) {
	client, err := getClient(host, port)
	if err != nil {
		return nil, err
	}
	// build query
	res, err := client.
		IndexStats(index).
		Do()
	if err != nil {
		return nil, fmt.Errorf("Error occured while querying index stats: %v", err)
	}
	return res, nil
}

// Scan returns a scan cursor to page through all documents of an index.
func Scan(host string, port string, index string, size int) (*elastic.ScanCursor, error) {
	client, err := getClient(host, port)
	if err != nil {
		return nil, err
	}
	// build query
	res, err := client.
		Scan(index).
		Size(size).
		Do()
	if err != nil {
		return nil, fmt.Errorf("Error occured whiling scanning: %v", err)
	}
	return res, nil
}

// Search returns the number of docs in an index.
func Search(host string, port string, index string, size int) (*elastic.SearchResult, error) {
	client, err := getClient(host, port)
	if err != nil {
		return nil, err
	}
	res, err := client.
		Search().
		Index(index).
		Size(size).
		Do()
	if err != nil {
		return nil, fmt.Errorf("Error occured while searching: %v", err)
	}
	return res, nil
}

// GetMapping returns the mapping for the provided index.
func GetMapping(host string, port string, index string) (string, error) {
	// get client
	client, err := getClient(host, port)
	if err != nil {
		return "", err
	}
	// get mapping
	result, err := client.
		GetMapping().
		Index(index).
		Do()
	if err != nil {
		return "", fmt.Errorf("Error occured while retrieving mapping: %v", err)
	}
	indexRaw, ok := result[index]
	if !ok {
		return "", fmt.Errorf("Could not find mapping for index `%s` in response", index)
	}
	indexMap, ok := indexRaw.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("Could not cast mapping into map[string]interface{}")
	}
	mappings, ok := indexMap["mappings"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("Could not find mappings for index `%s` in response", index)
	}
	bytes, err := json.Marshal(mappings)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
