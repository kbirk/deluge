package document

import (
	"fmt"
	"strconv"

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

// ColumnExists returns true if the provided column index exists in the row.
func (d *CSV) ColumnExists(index int) bool {
	if index > len(d.Cols)-1 {
		return false
	}
	col := d.Cols[index]
	if col != "" && col != "null" {
		return true
	}
	return false
}

// Float64 returns the column as a float64.
func (d *CSV) Float64(index int) (float64, bool) {
	if d.ColumnExists(index) {
		val, err := strconv.ParseFloat(d.Cols[index], 64)
		if err == nil {
			return val, true
		}
	}
	return 0, false
}

// Float32 returns the column as a float32.
func (d *CSV) Float32(index int) (float32, bool) {
	if d.ColumnExists(index) {
		val, err := strconv.ParseFloat(d.Cols[index], 32)
		if err == nil {
			return float32(val), true
		}
	}
	return 0, false
}

// Int64 returns the column as an int64.
func (d *CSV) Int64(index int) (int64, bool) {
	if d.ColumnExists(index) {
		val, err := strconv.ParseInt(d.Cols[index], 10, 64)
		if err == nil {
			return val, true
		}
	}
	return 0, false
}

// Int32 returns the column as an int32.
func (d *CSV) Int32(index int) (int32, bool) {
	if d.ColumnExists(index) {
		val, err := strconv.ParseInt(d.Cols[index], 10, 32)
		if err == nil {
			return int32(val), true
		}
	}
	return 0, false
}

// String returns the column as a string.
func (d *CSV) String(index int) (string, bool) {
	if d.ColumnExists(index) {
		return d.Cols[index], true
	}
	return "", false
}

// Bool returns the column as a bool.
func (d *CSV) Bool(index int) (bool, bool) {
	if d.ColumnExists(index) {
		col := d.Cols[index]
		if col == "true" || col == "1" {
			return true, true
		}
		return false, true
	}
	return false, false
}
