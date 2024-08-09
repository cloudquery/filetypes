package parquet

import (
	"testing"

	"github.com/cloudquery/codegen/jsonschema"
	"github.com/stretchr/testify/require"
)

func TestSpec_JSONSchema(t *testing.T) {
	schema, err := jsonschema.Generate(ParquetSpec{})
	require.NoError(t, err)

	jsonschema.TestJSONSchema(t, string(schema), []jsonschema.TestCase{
		{
			Name: "empty",
			Spec: `{}`,
		},
		{
			Name: "extra keyword",
			Err:  true,
			Spec: `{"extra":true}`,
		},
		{
			Name:         "invalid version",
			ErrorMessage: "at '/version': value must be one of 'v1.0', 'v2.4', 'v2.6', 'v2Latest'",
			Spec:         `{"version":"invalid"}`,
		},
		{
			Name: "valid version",
			Spec: `{"version":"v1.0"}`,
		},
		{
			Name: "valid root_repetition",
			Spec: `{"root_repetition":"undefined"}`,
		},
		{
			Name:         "invalid root_repetition",
			ErrorMessage: "at '/root_repetition': value must be one of 'undefined', 'required', 'optional', 'repeated'",
			Spec:         `{"root_repetition":"invalid"}`,
		},
	})
}
