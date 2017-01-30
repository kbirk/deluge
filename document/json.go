package document

import (
	"encoding/json"
	"fmt"
	"strconv"
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
	err := json.Unmarshal([]byte(line), &m)
	if err != nil {
		return fmt.Errorf("Could not unmarshal `%v` into type map[string]interface{}", data)
	}
	d.Data = m
	return nil
}

// Get returns an interface{} under the given path.
func (d *JSON) Get(path ...string) (interface{}, bool) {
	child := d.Data
	last := len(path) - 1
	var val interface{} = child
	for index, key := range path {
		// does a child exists?
		v, ok := child[key]
		if !ok {
			return nil, false
		}
		// is it the target?
		if index == last {
			val = v
			break
		}
		// if not, does it have children to traverse?
		c, ok := v.(map[string]interface{})
		if !ok {
			return nil, false
		}
		child = c
	}
	return val, true
}

// Exists returns true if something exists under the provided path.
func (d *JSON) Exists(path ...string) bool {
	_, ok := d.Get(path...)
	return ok
}

// Child returns the child under the given path.
func (d *JSON) Child(path ...string) (map[string]interface{}, bool) {
	c, ok := d.Get(path...)
	if !ok {
		return nil, false
	}
	child, ok := c.(map[string]interface{})
	if !ok {
		return nil, false
	}
	return child, true
}

// String returns a string property under the given path.
func (d *JSON) String(path ...string) (string, bool) {
	v, ok := d.Get(path...)
	if !ok {
		return "", false
	}
	val, ok := v.(string)
	if !ok {
		return "", false
	}
	return val, true
}

// Float64 returns a float64 property under the given path.
func (d *JSON) Float64(path ...string) (float64, bool) {
	v, ok := d.Get(path...)
	if !ok {
		return 0, false
	}
	val, ok := v.(float64)
	if !ok {
		// if it is a string value, cast it to float64
		strval, ok := v.(string)
		if ok {
			val, err := strconv.ParseFloat(strval, 64)
			if err == nil {
				return val, true
			}
		}
		return 0, false
	}
	return val, true
}

// Bool returns a bool property under the given path.
func (d *JSON) Bool(path ...string) (bool, bool) {
	v, ok := d.Get(path...)
	if !ok {
		return false, false
	}
	val, ok := v.(bool)
	if !ok {
		return false, false
	}
	return val, true
}

// Array returns an []interface{} property under the given path.
func (d *JSON) Array(path ...string) ([]interface{}, bool) {
	v, ok := d.Get(path...)
	if !ok {
		return nil, false
	}
	val, ok := v.([]interface{})
	if !ok {
		return nil, false
	}
	return val, true
}

// ChildArray returns a []map[string]interface{} from the given path.
func (d *JSON) ChildArray(path ...string) ([]map[string]interface{}, bool) {
	vs, ok := d.Array(path...)
	if !ok {
		return nil, false
	}
	nodes := make([]map[string]interface{}, len(vs))
	for i, v := range vs {
		val, ok := v.(map[string]interface{})
		if !ok {
			return nil, false
		}
		nodes[i] = val
	}
	return nodes, true
}

// StringArray returns a []string property under the given path.
func (d *JSON) StringArray(path ...string) ([]string, bool) {
	vs, ok := d.Array(path...)
	if !ok {
		return nil, false
	}
	strs := make([]string, len(vs))
	for i, v := range vs {
		val, ok := v.(string)
		if !ok {
			return nil, false
		}
		strs[i] = val
	}
	return strs, true
}

// Float64Array returns a []float64 property under the given path.
func (d *JSON) Float64Array(path ...string) ([]float64, bool) {
	vs, ok := d.Array(path...)
	if !ok {
		return nil, false
	}
	flts := make([]float64, len(vs))
	for i, v := range vs {
		val, ok := v.(float64)
		if !ok {
			return nil, false
		}
		flts[i] = val
	}
	return flts, true
}

// BoolArray returns a []bool property under the given path.
func (d *JSON) BoolArray(path ...string) ([]bool, bool) {
	vs, ok := d.Array(path...)
	if !ok {
		return nil, false
	}
	bools := make([]bool, len(vs))
	for i, v := range vs {
		val, ok := v.(bool)
		if !ok {
			return nil, false
		}
		bools[i] = val
	}
	return bools, true
}
