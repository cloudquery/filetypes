package csv

import (
	"errors"

	"github.com/invopop/jsonschema"
)

// nolint:revive
type CSVSpec struct {
	SkipHeader bool   `json:"skip_header,omitempty"`
	Delimiter  string `json:"delimiter,omitempty"`
}

func (CSVSpec) JSONSchema() *jsonschema.Schema {
	properties := jsonschema.NewProperties()
	properties.Set("skip_header", &jsonschema.Schema{
		Type:        "boolean",
		Description: "Specifies if the first line of a file should be the header.",
		Default:     false,
	})
	properties.Set("delimiter", &jsonschema.Schema{
		Type:        "string",
		Description: "Character that will be used as the delimiter.",
		Pattern:     `^.$`, // a single character
		Default:     ",",
	})
	return &jsonschema.Schema{
		Description:          "CloudQuery CSV file output spec.",
		Properties:           properties,
		Type:                 "object",
		AdditionalProperties: jsonschema.FalseSchema, // "additionalProperties": false
	}
}

func (s *CSVSpec) SetDefaults() {
	if s.Delimiter == "" {
		s.Delimiter = ","
	}
}

func (s *CSVSpec) Validate() error {
	if len(s.Delimiter) != 1 {
		return errors.New("delimiter must be a single character")
	}
	return nil
}
