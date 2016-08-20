package dsv

import (
	"errors"
)

// ParseFields will parse a delimited string into separate fields.
func ParseFields(row string, delimiter rune) ([]string, error) {
	isQuoted := false
	var fields []string
	field := ""
	for _, c := range row {
		switch c {
		case delimiter:
			if isQuoted {
				field += string(c)
			} else {
				fields = append(fields, field)
				field = ""
			}
		case '\n':
			return nil, errors.New("Line ending found in row")
		case '"':
			isQuoted = !isQuoted
		default:
			field += string(c)
		}
	}
	// add remaining field
	if len(field) > 0 {
		fields = append(fields, field)
	}
	return fields, nil
}
