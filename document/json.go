package document

import (
	"encoding/json"
	"fmt"
)

// JSON represents a basic json based document.
type JSON struct {
	Data map[string]interface{}
}

// SetData sets the internal JSON data.
func (d *JSON) SetData(data interface{}) error {
	// cast back to a string
	line, ok := data.(string)
	if !ok {
		return fmt.Errorf("Could not cast `%v` into type string", data)
	}
	// unmarshal
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		return fmt.Errorf("Could not unmarshal `%v` into type map[string]interface{}", data)
	}
	d.Data = m
	return nil
}
