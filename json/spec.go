package json

import "github.com/invopop/jsonschema"

// nolint:revive
type JSONSpec struct{}

func (JSONSpec) JSONSchema() *jsonschema.Schema {
	return &jsonschema.Schema{
		Description:          "CloudQuery JSON file output spec.",
		Type:                 "object",
		AdditionalProperties: jsonschema.FalseSchema, // "additionalProperties": false
	}
}

func (*JSONSpec) SetDefaults() {}

func (*JSONSpec) Validate() error {
	return nil
}
