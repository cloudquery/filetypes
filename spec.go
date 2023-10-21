package filetypes

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/cloudquery/filetypes/v4/csv"
	jsonFile "github.com/cloudquery/filetypes/v4/json"
	"github.com/cloudquery/filetypes/v4/parquet"
	"github.com/invopop/jsonschema"
	orderedmap "github.com/wk8/go-ordered-map/v2"
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

	csvSpec     *csv.Spec
	jsonSpec    *jsonFile.Spec
	parquetSpec *parquet.Spec
}

func (FileSpec) JSONSchemaExtend(sc *jsonschema.Schema) {
	sc.ID = "/schemas/FileSpec"
	sc.Definitions = jsonschema.Definitions{
		"csv":     csv.Spec{}.JSONSchema(),
		"json":    jsonFile.Spec{}.JSONSchema(),
		"parquet": parquet.Spec{}.JSONSchema(),
	}

	sc.Properties.Set("format_spec", &jsonschema.Schema{
		OneOf: []*jsonschema.Schema{
			{
				AnyOf: []*jsonschema.Schema{
					{Ref: jsonschema.EmptyID.Def("csv").String()},
					{Ref: jsonschema.EmptyID.Def("json").String()},
					{Ref: jsonschema.EmptyID.Def("parquet").String()},
				},
			},
			{Type: "null"},
		},
	})

	// now we need to enforce format -> specific type
	sc.OneOf = []*jsonschema.Schema{
		// CSV
		{
			Properties: func() *orderedmap.OrderedMap[string, *jsonschema.Schema] {
				properties := jsonschema.NewProperties()
				properties.Set("format", &jsonschema.Schema{Type: "string", Const: FormatTypeCSV})
				properties.Set("format_spec", &jsonschema.Schema{
					OneOf: []*jsonschema.Schema{
						{Ref: jsonschema.EmptyID.Def("csv").String()},
						{Type: "null"},
					},
				})
				return properties
			}(),
		},
		// JSON
		{
			Properties: func() *orderedmap.OrderedMap[string, *jsonschema.Schema] {
				properties := jsonschema.NewProperties()
				properties.Set("format", &jsonschema.Schema{Type: "string", Const: FormatTypeJSON})
				properties.Set("format_spec", &jsonschema.Schema{
					OneOf: []*jsonschema.Schema{
						{Ref: jsonschema.EmptyID.Def("json").String()},
						{Type: "null"},
					},
				})
				return properties
			}(),
		},
		// Parquet
		{
			Properties: func() *orderedmap.OrderedMap[string, *jsonschema.Schema] {
				properties := jsonschema.NewProperties()
				properties.Set("format", &jsonschema.Schema{Type: "string", Const: FormatTypeParquet})
				properties.Set("format_spec", &jsonschema.Schema{
					OneOf: []*jsonschema.Schema{
						{Ref: jsonschema.EmptyID.Def("parquet").String()},
						{Type: "null"},
					},
				})
				return properties
			}(),
		},
	}
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
