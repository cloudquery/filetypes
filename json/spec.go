package json

import "github.com/invopop/jsonschema"

type Spec struct{}

func (Spec) JSONSchema() *jsonschema.Schema {
	return &jsonschema.Schema{
		Description:          "CloudQuery JSON file output spec.",
		Type:                 "object",
		AdditionalProperties: jsonschema.FalseSchema, // "additionalProperties": false
	}
}

func (*Spec) SetDefaults() {}

func (*Spec) Validate() error {
	return nil
}
