package filetypes

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/cloudquery/filetypes/v4/csv"
	jsonfile "github.com/cloudquery/filetypes/v4/json"
	"github.com/cloudquery/filetypes/v4/parquet"
)

type FormatType string

const (
	FormatTypeCSV     = "csv"
	FormatTypeJSON    = "json"
	FormatTypeParquet = "parquet"
)

// Compression type.
type CompressionType string

const (
	CompressionTypeNone CompressionType = ""
	CompressionTypeGZip CompressionType = "gzip"
)

type FileSpec struct {
	// Output format.
	Format FormatType `json:"format,omitempty" jsonschema:"required,enum=csv,enum=json,enum=parquet"`

	// Format spec.
	FormatSpec any `json:"format_spec,omitempty"`

	// Compression type.
	// Empty or missing stands for no compression.
	Compression CompressionType `json:"compression,omitempty" jsonschema:"enum=,enum=gzip"`

	csvSpec     *csv.CSVSpec
	jsonSpec    *jsonfile.JSONSpec
	parquetSpec *parquet.ParquetSpec
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
	if !s.Compression.IsValid() {
		return fmt.Errorf("`compression` must be either empty or `%s`", CompressionTypeGZip)
	}
	if s.Format == "" {
		return fmt.Errorf("format is required")
	}
	switch s.Format {
	case FormatTypeCSV:
		return s.csvSpec.Validate()
	case FormatTypeJSON:
		return s.jsonSpec.Validate()
	case FormatTypeParquet:
		if s.Compression != CompressionTypeNone {
			return fmt.Errorf("compression is not supported for parquet format") // This won't work even if we wanted to, because parquet writer prematurely closes the file handle
		}

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
		s.csvSpec = &csv.CSVSpec{}
		return dec.Decode(s.csvSpec)
	case FormatTypeJSON:
		s.jsonSpec = &jsonfile.JSONSpec{}
		return dec.Decode(s.jsonSpec)
	case FormatTypeParquet:
		s.parquetSpec = &parquet.ParquetSpec{}
		return dec.Decode(s.parquetSpec)
	default:
		return fmt.Errorf("unknown format %s", s.Format)
	}
}

func (c CompressionType) IsValid() bool {
	switch c {
	case CompressionTypeNone, CompressionTypeGZip:
		return true
	default:
		return false
	}
}

func (c CompressionType) Extension() string {
	switch c {
	case CompressionTypeGZip:
		return ".gz"
	default:
		return ""
	}
}
