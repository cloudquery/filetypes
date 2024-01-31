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
		preDefaultsCSV   *csv.CSVSpec
		preDefaultsJSON  *json.JSONSpec
		postDefaultsCSV  *csv.CSVSpec
		postDefaultsJSON *json.JSONSpec
		expectError      bool
	}{
		{
			FileSpec: &FileSpec{
				Format:     FormatTypeCSV,
				FormatSpec: map[string]any{},
			},
			preDefaultsCSV: &csv.CSVSpec{},
			postDefaultsCSV: &csv.CSVSpec{
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
			preDefaultsCSV: &csv.CSVSpec{
				SkipHeader: true,
				Delimiter:  ",",
			},
			postDefaultsCSV: &csv.CSVSpec{
				SkipHeader: true,
				Delimiter:  ",",
			},
		},
		{
			FileSpec: &FileSpec{
				Format:     FormatTypeCSV,
				FormatSpec: map[string]any{},
			},
			preDefaultsCSV: &csv.CSVSpec{
				SkipHeader: false,
				Delimiter:  "",
			},
			postDefaultsCSV: &csv.CSVSpec{
				SkipHeader: false,
				Delimiter:  ",",
			},
		},
		{
			FileSpec: &FileSpec{
				Format: FormatTypeJSON,
			},
			preDefaultsJSON:  &json.JSONSpec{},
			postDefaultsJSON: &json.JSONSpec{},
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
