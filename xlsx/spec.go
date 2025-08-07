package xlsx

import (
	"github.com/invopop/jsonschema"
)

type Spec struct{}

func (Spec) JSONSchema() *jsonschema.Schema {
	properties := jsonschema.NewProperties()
	return &jsonschema.Schema{
		Description:          "CloudQuery XLSX file output spec.",
		Properties:           properties,
		Type:                 "object",
		AdditionalProperties: jsonschema.FalseSchema, // "additionalProperties": false
	}
}

func (s *Spec) SetDefaults() {}

func (s *Spec) Validate() error {
	return nil
}
