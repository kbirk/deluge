package document

import (
	"fmt"

	"github.com/unchartedsoftware/deluge/util/dsv"
)

// CSV represents a basic csv based document.
type CSV struct {
	Cols []string
}

// SetData sets the internal CSV column.
func (d *CSV) SetData(data interface{}) error {
	// cast back to a string
	line, ok := data.(string)
	if !ok {
		return fmt.Errorf("Could not cast `%v` into type string", data)
	}
	// parse delimited fields
	cols, err := dsv.ParseFields(line, ',')
	if err != nil {
		return err
	}
	d.Cols = cols
	return nil
}
