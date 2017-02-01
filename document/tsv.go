package document

import (
	"fmt"
	"strconv"
	"strings"

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

// ColumnExists returns true if the provided column index exists in the row.
func (d *TSV) ColumnExists(index int) bool {
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
func (d *TSV) Float64(index int) (float64, bool) {
	if d.ColumnExists(index) {
		val, err := strconv.ParseFloat(d.Cols[index], 64)
		if err == nil {
			return val, true
		}
	}
	return 0, false
}

// Float32 returns the column as a float32.
func (d *TSV) Float32(index int) (float32, bool) {
	if d.ColumnExists(index) {
		val, err := strconv.ParseFloat(d.Cols[index], 32)
		if err == nil {
			return float32(val), true
		}
	}
	return 0, false
}

// Int64 returns the column as an int64.
func (d *TSV) Int64(index int) (int64, bool) {
	if d.ColumnExists(index) {
		val, err := strconv.ParseInt(d.Cols[index], 10, 64)
		if err == nil {
			return val, true
		}
	}
	return 0, false
}

// Int32 returns the column as an int32.
func (d *TSV) Int32(index int) (int32, bool) {
	if d.ColumnExists(index) {
		val, err := strconv.ParseInt(d.Cols[index], 10, 32)
		if err == nil {
			return int32(val), true
		}
	}
	return 0, false
}

// Int returns the column as an int.
func (d *TSV) Int(index int) (int, bool) {
	if d.ColumnExists(index) {
		val, err := strconv.ParseInt(d.Cols[index], 10, 64)
		if err == nil {
			return int(val), true
		}
	}
	return 0, false
}

// String returns the column as a string.
func (d *TSV) String(index int) (string, bool) {
	if d.ColumnExists(index) {
		return d.Cols[index], true
	}
	return "", false
}

// Bool returns the column as a bool.
func (d *TSV) Bool(index int) (bool, bool) {
	if d.ColumnExists(index) {
		col := d.Cols[index]
		if col == "true" || col == "1" {
			return true, true
		}
		return false, true
	}
	return false, false
}

// Splits a string if the column exists.
func (d *TSV) SplitString(index int, delim string) ([]string, bool) {
	if d.ColumnExists(index) {
		str := d.Cols[index]
		if len(str) > 0 {
			return strings.Split(str, delim), true
		}
	}
	return nil, false
}

// Ints returns the column as an int slice.
func (d *TSV) SplitInt(index int, delim string) ([]int, bool) {
	strings, success := d.SplitString(index, delim)

	if strings != nil {
		// Parse the strings one by one.
		ints := make([]int, len(strings))
		for i := 0; i < len(strings); i++ {
			val, err := strconv.ParseInt(strings[index], 10, 64)
			if err == nil {
				ints[i] = int(val)
			} else {
				return nil, false
			}
		}

		return ints, true
	}

	return nil, success
}

// Ints returns the column as an int slice.
func (d *TSV) SplitFloat64(index int, delim string) ([]float64, bool) {
	strings, success := d.SplitString(index, delim)

	if strings != nil {
		// Parse the strings one by one.
		floats := make([]float64, len(strings))
		for i := 0; i < len(strings); i++ {
			val, err := strconv.ParseFloat(strings[index], 64)
			if err == nil {
				floats[i] = val
			} else {
				return nil, false
			}
		}

		return floats, true
	}

	return nil, success
}
