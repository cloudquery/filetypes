package filetypes

import (
	"fmt"

	"github.com/cloudquery/filetypes/csv"
	"github.com/cloudquery/filetypes/json"
)

type FormatType string

const (
	FormatTypeCSV  = "csv"
	FormatTypeJSON = "json"
)

type FileSpec struct {
	Format FormatType `json:"format,omitempty"`
	*csv.CSVSpec
	*json.JSONSpec
}

func (s *FileSpec) SetDefaults() {
	switch s.Format {
	case FormatTypeCSV:
		s.CSVSpec.SetDefaults()
	case FormatTypeJSON:
		s.JSONSpec.SetDefaults()
	}
}
func (s *FileSpec) Validate() error {
	if s.Format == "" {
		return fmt.Errorf("format is required")
	}
	switch s.Format {
	case FormatTypeCSV:
		return s.CSVSpec.Validate()
	case FormatTypeJSON:
		return s.JSONSpec.Validate()
	default:
		return fmt.Errorf("unknown format %s", s.Format)
	}
}

type Client struct {
	spec                   *FileSpec
	csv                    *csv.Client
	json                   *json.Client
	csvTransformer         *csv.Transformer
	csvReverseTransformer  *csv.ReverseTransformer
	jsonTransformer        *json.Transformer
	jsonReverseTransformer *json.ReverseTransformer
}

// NewClient creates a new client for the given spec
func NewClient(spec *FileSpec) (*Client, error) {
	switch spec.Format {
	case FormatTypeCSV:
		opts := []csv.Options{
			csv.WithDelimiter([]rune(spec.Delimiter)[0]),
		}
		if spec.IncludeHeaders {
			opts = append(opts, csv.WithHeader())
		}

		client, err := csv.NewClient(opts...)
		if err != nil {
			return &Client{}, err
		}
		return &Client{
			spec:                  spec,
			csvTransformer:        &csv.Transformer{},
			csvReverseTransformer: &csv.ReverseTransformer{},
			csv:                   client,
		}, nil

	case FormatTypeJSON:
		client, err := json.NewClient()
		if err != nil {
			return &Client{}, err
		}
		return &Client{
			spec:                   spec,
			jsonTransformer:        &json.Transformer{},
			jsonReverseTransformer: &json.ReverseTransformer{},
			json:                   client,
		}, nil

	default:
		panic("unknown format " + spec.Format)
	}
}
