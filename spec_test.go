package filetypes

import (
	"testing"

	"github.com/cloudquery/filetypes/v4/csv"
	"github.com/cloudquery/filetypes/v4/json"
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
