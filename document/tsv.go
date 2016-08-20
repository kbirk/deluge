package document

import (
	"fmt"

	"github.com/unchartedsoftware/deluge/util/dsv"
)

// TSV represents a basic tsv based document.
type TSV struct {
	Cols []string
}

// SetData sets the internal TSV column.
func (d *TSV) SetData(data interface{}) error {
	// cast back to a string
	line, ok := data.(string)
	if !ok {
		return fmt.Errorf("Could not cast `%v` into type string", data)
	}
	// parse delimited fields
	cols, err := dsv.ParseFields(line, '\t')
	if err != nil {
		return err
	}
	d.Cols = cols
	return nil
}
