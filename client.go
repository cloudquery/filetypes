package filetypes

import (
	"github.com/cloudquery/filetypes/csv"
	"github.com/cloudquery/filetypes/json"
)

type FormatType string

const (
	FormatTypeCSV  = "csv"
	FormatTypeJSON = "json"
)

type FileSpec struct {
	Format         FormatType `json:"format,omitempty"`
	IncludeHeaders bool       `json:"include_headers,omitempty"`
	Delimiter      rune       `json:"delimiter,omitempty"`
	NoRotate       bool       `json:"no_rotate,omitempty"`
}

func (s *FileSpec) SetDefaults() {
	if s.Delimiter == 0 {
		s.Delimiter = ','
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
			csv.WithDelimiter(spec.Delimiter),
		}
		if spec.IncludeHeaders {
			opts = append(opts, csv.WithHeader())
		}

		client, err := csv.NewClient(opts...)
		if err != nil {
			return &Client{}, err
		}
		return &Client{
			spec:                   spec,
			csvTransformer:         &csv.Transformer{},
			jsonTransformer:        &json.Transformer{},
			csvReverseTransformer:  &csv.ReverseTransformer{},
			jsonReverseTransformer: &json.ReverseTransformer{},
			csv:                    client,
		}, nil

	case FormatTypeJSON:
		client, err := json.NewClient()
		if err != nil {
			return &Client{}, err
		}
		return &Client{
			spec:                   spec,
			csvTransformer:         &csv.Transformer{},
			jsonTransformer:        &json.Transformer{},
			csvReverseTransformer:  &csv.ReverseTransformer{},
			jsonReverseTransformer: &json.ReverseTransformer{},
			json:                   client,
		}, nil

	default:
		panic("unknown format " + spec.Format)
	}
}
