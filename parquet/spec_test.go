package parquet

import (
	"testing"

	"github.com/cloudquery/codegen/jsonschema"
	"github.com/stretchr/testify/require"
)

func TestSpec_JSONSchema(t *testing.T) {
	schema, err := jsonschema.Generate(Spec{})
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
	})
}
