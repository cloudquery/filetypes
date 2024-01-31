package parquet

import "github.com/invopop/jsonschema"

// nolint:revive
type ParquetSpec struct{}

func (ParquetSpec) JSONSchema() *jsonschema.Schema {
	return &jsonschema.Schema{
		Description:          "CloudQuery Parquet file output spec.",
		Type:                 "object",
		AdditionalProperties: jsonschema.FalseSchema, // "additionalProperties": false
	}
}

func (*ParquetSpec) SetDefaults() {
}

func (*ParquetSpec) Validate() error {
	return nil
}
