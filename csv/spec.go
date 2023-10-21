package csv

import (
	"fmt"

	"github.com/invopop/jsonschema"
)

type Spec struct {
	SkipHeader bool   `json:"skip_header,omitempty"`
	Delimiter  string `json:"delimiter,omitempty"`
}

func (Spec) JSONSchema() *jsonschema.Schema {
	properties := jsonschema.NewProperties()
	properties.Set("skip_header", &jsonschema.Schema{
		Type:        "boolean",
		Description: "Specifies if the first line of a file should be the header.",
		Default:     false,
	})
	properties.Set("delimiter", &jsonschema.Schema{
		Type:        "string",
		Description: "Character that will be used as want to use as the delimiter.",
		Pattern:     `^.$`, // a single character
		Default:     ",",
	})
	return &jsonschema.Schema{
		Anchor:               "csv-spec",
		Description:          "CloudQuery CSV file output spec",
		Properties:           properties,
		Type:                 "object",
		AdditionalProperties: jsonschema.FalseSchema, // "additionalProperties": false
	}
}

func (s *Spec) SetDefaults() {
	if s.Delimiter == "" {
		s.Delimiter = ","
	}
}

func (s *Spec) Validate() error {
	if len(s.Delimiter) != 1 {
		return fmt.Errorf("delimiter must be a single character")
	}
	return nil
}
