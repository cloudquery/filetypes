package filetypes

import (
	"testing"

	"github.com/cloudquery/codegen/jsonschema"
	"github.com/cloudquery/filetypes/v4/csv"
	"github.com/cloudquery/filetypes/v4/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpecMethods(t *testing.T) {
	testCases := []struct {
		FileSpec         *FileSpec
		preDefaultsCSV   *csv.Spec
		preDefaultsJSON  *json.Spec
		postDefaultsCSV  *csv.Spec
		postDefaultsJSON *json.Spec
		expectError      bool
	}{
		{
			FileSpec: &FileSpec{
				Format:     FormatTypeCSV,
				FormatSpec: map[string]any{},
			},
			preDefaultsCSV: &csv.Spec{},
			postDefaultsCSV: &csv.Spec{
				SkipHeader: false,
				Delimiter:  ",",
			},
		},
		{
			FileSpec: &FileSpec{
				Format: FormatTypeCSV,
				FormatSpec: map[string]any{
					"delimiter":   ",",
					"skip_header": true,
				},
			},
			preDefaultsCSV: &csv.Spec{
				SkipHeader: true,
				Delimiter:  ",",
			},
			postDefaultsCSV: &csv.Spec{
				SkipHeader: true,
				Delimiter:  ",",
			},
		},
		{
			FileSpec: &FileSpec{
				Format:     FormatTypeCSV,
				FormatSpec: map[string]any{},
			},
			preDefaultsCSV: &csv.Spec{
				SkipHeader: false,
				Delimiter:  "",
			},
			postDefaultsCSV: &csv.Spec{
				SkipHeader: false,
				Delimiter:  ",",
			},
		},
		{
			FileSpec: &FileSpec{
				Format: FormatTypeJSON,
			},
			preDefaultsJSON:  &json.Spec{},
			postDefaultsJSON: &json.Spec{},
		},

		{
			FileSpec: &FileSpec{
				Format: FormatTypeJSON,
				FormatSpec: map[string]any{
					"delimiter": ",",
				},
			},
			expectError: true,
		},
		{
			FileSpec: &FileSpec{
				Format: FormatTypeCSV,
				FormatSpec: map[string]any{
					"delimiter22": ",",
				},
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		err := tc.FileSpec.UnmarshalSpec()
		if tc.expectError {
			assert.NotNil(t, err)
			continue
		}
		assert.Equal(t, tc.preDefaultsCSV, tc.FileSpec.csvSpec)
		assert.Equal(t, tc.preDefaultsJSON, tc.FileSpec.jsonSpec)

		tc.FileSpec.SetDefaults()

		assert.Equal(t, tc.postDefaultsCSV, tc.FileSpec.csvSpec)
		assert.Equal(t, tc.postDefaultsJSON, tc.FileSpec.jsonSpec)
	}
}

func TestFileSpec_JSONSchemaExtend(t *testing.T) {
	schema, err := jsonschema.Generate(FileSpec{})
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
