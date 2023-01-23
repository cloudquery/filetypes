package filetypes

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/cloudquery/filetypes/csv"
	jsonFile "github.com/cloudquery/filetypes/json"
)

type FormatType string

const (
	FormatTypeCSV  = "csv"
	FormatTypeJSON = "json"
)

type FileSpec struct {
	Format     FormatType `json:"format,omitempty"`
	FormatSpec any        `json:"format_spec,omitempty"`
	csvSpec    *csv.CSVSpec
	jsonSpec   *jsonFile.JSONSpec
}

func (s *FileSpec) SetDefaults() {
	switch s.Format {
	case FormatTypeCSV:
		s.csvSpec.SetDefaults()
	case FormatTypeJSON:
		s.jsonSpec.SetDefaults()
	}
}
func (s *FileSpec) Validate() error {
	if s.Format == "" {
		return fmt.Errorf("format is required")
	}
	switch s.Format {
	case FormatTypeCSV:
		return s.csvSpec.Validate()
	case FormatTypeJSON:
		return s.jsonSpec.Validate()
	default:
		return fmt.Errorf("unknown format %s", s.Format)
	}
}

func (s *FileSpec) UnmarshalSpec() error {
	b, err := json.Marshal(s.FormatSpec)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	dec.DisallowUnknownFields()

	switch s.Format {
	case FormatTypeCSV:
		s.csvSpec = &csv.CSVSpec{}
		return dec.Decode(s.csvSpec)
	case FormatTypeJSON:
		s.jsonSpec = &jsonFile.JSONSpec{}
		return dec.Decode(s.jsonSpec)
	default:
		return fmt.Errorf("unknown format %s", s.Format)
	}
}
