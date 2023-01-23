package filetypes

import (
	"testing"

	"github.com/cloudquery/filetypes/csv"
	"github.com/cloudquery/filetypes/json"
	"github.com/stretchr/testify/assert"
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
				IncludeHeaders: false,
				Delimiter:      ",",
			},
		},
		{
			FileSpec: &FileSpec{
				Format: FormatTypeCSV,
				FormatSpec: map[string]any{
					"delimiter":       ",",
					"include_headers": true,
				},
			},
			preDefaultsCSV: &csv.Spec{
				IncludeHeaders: true,
				Delimiter:      ",",
			},
			postDefaultsCSV: &csv.Spec{
				IncludeHeaders: true,
				Delimiter:      ",",
			},
		},
		{
			FileSpec: &FileSpec{
				Format:     FormatTypeCSV,
				FormatSpec: map[string]any{},
			},
			preDefaultsCSV: &csv.Spec{
				IncludeHeaders: false,
				Delimiter:      "",
			},
			postDefaultsCSV: &csv.Spec{
				IncludeHeaders: false,
				Delimiter:      ",",
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
