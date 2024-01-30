package filetypes

import (
	"testing"

	"github.com/cloudquery/codegen/jsonschema"
	"github.com/stretchr/testify/require"
)

func TestFileSpec_JSONSchemaExtend(t *testing.T) {
	schema, err := jsonschema.Generate(FileSpec{}, FileSpec{}.JSONSchemaOptions()...)
	require.NoError(t, err)

	jsonschema.TestJSONSchema(t, string(schema), []jsonschema.TestCase{
		{
			Name: "empty",
			Err:  true, // missing format
			Spec: `{}`,
		},
		{
			Name: "empty format",
			Err:  true,
			Spec: `{"format":""}`,
		},
		{
			Name: "null format",
			Err:  true,
			Spec: `{"format":null}`,
		},
		{
			Name: "bad format",
			Err:  true,
			Spec: `{"format":123}`,
		},
		{
			Name: "bad format value",
			Err:  true,
			Spec: `{"format":"abc"}`,
		},
		{
			Name: "csv format",
			Spec: `{"format":"csv"}`,
		},
		{
			Name: "csv format + empty format_spec",
			Spec: `{"format":"csv","format_spec":{}}`,
		},
		{
			Name: "csv format + null format_spec",
			Spec: `{"format":"csv","format_spec":null}`,
		},
		{
			Name: "csv format + csv format_spec",
			Spec: `{"format":"csv","format_spec":{"skip_header": true, "delimiter":","}}`,
		},
		{
			Name: "json format",
			Spec: `{"format":"json"}`,
		},
		{
			Name: "json format + empty format_spec",
			Spec: `{"format":"json","format_spec":{}}`,
		},
		{
			Name: "json format + null format_spec",
			Spec: `{"format":"json","format_spec":null}`,
		},
		{
			Name: "json format + csv format_spec",
			Err:  true,
			Spec: `{"format":"json","format_spec":{"skip_header": true, "delimiter":","}}`,
		},
		{
			Name: "parquet format",
			Spec: `{"format":"parquet"}`,
		},
		{
			Name: "parquet format + empty format_spec",
			Spec: `{"format":"parquet","format_spec":{}}`,
		},
		{
			Name: "parquet format + null format_spec",
			Spec: `{"format":"parquet","format_spec":null}`,
		},
		{
			Name: "parquet format + csv format_spec",
			Err:  true,
			Spec: `{"format":"parquet","format_spec":{"skip_header": true, "delimiter":","}}`,
		},
	})
}
