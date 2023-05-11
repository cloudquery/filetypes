package filetypes

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/cloudquery/filetypes/v3/csv"
	jsonFile "github.com/cloudquery/filetypes/v3/json"
	"github.com/cloudquery/filetypes/v3/parquet"
)

type FormatType string

const (
	FormatTypeCSV     = "csv"
	FormatTypeJSON    = "json"
	FormatTypeParquet = "parquet"
)

type FileSpec struct {
	Format      FormatType `json:"format,omitempty"`
	FormatSpec  any        `json:"format_spec,omitempty"`
	csvSpec     *csv.Spec
	jsonSpec    *jsonFile.Spec
	parquetSpec *parquet.Spec
}

func (s *FileSpec) SetDefaults() {
	switch s.Format {
	case FormatTypeCSV:
		s.csvSpec.SetDefaults()
	case FormatTypeJSON:
		s.jsonSpec.SetDefaults()
	case FormatTypeParquet:
		s.parquetSpec.SetDefaults()
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
	case FormatTypeParquet:
		return s.parquetSpec.Validate()
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
		s.csvSpec = &csv.Spec{}
		return dec.Decode(s.csvSpec)
	case FormatTypeJSON:
		s.jsonSpec = &jsonFile.Spec{}
		return dec.Decode(s.jsonSpec)
	case FormatTypeParquet:
		s.parquetSpec = &parquet.Spec{}
		return dec.Decode(s.parquetSpec)
	default:
		return fmt.Errorf("unknown format %s", s.Format)
	}
}
