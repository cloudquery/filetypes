package filetypes

import (
	csvFile "github.com/cloudquery/filetypes/csv"
	jsonFile "github.com/cloudquery/filetypes/json"
)

type Client struct {
	spec                   *FileSpec
	csv                    *csvFile.Client
	json                   *jsonFile.Client
	csvTransformer         csvFile.Transformer
	csvReverseTransformer  csvFile.ReverseTransformer
	jsonTransformer        jsonFile.Transformer
	jsonReverseTransformer jsonFile.ReverseTransformer
}

// NewClient creates a new client for the given spec
func NewClient(spec *FileSpec) (*Client, error) {
	err := spec.UnmarshalSpec()
	if err != nil {
		return &Client{}, err
	}

	spec.SetDefaults()
	if err := spec.Validate(); err != nil {
		return &Client{}, err
	}

	switch spec.Format {
	case FormatTypeCSV:
		opts := []csvFile.Options{
			csvFile.WithDelimiter([]rune(spec.csvSpec.Delimiter)[0]),
		}
		if !spec.csvSpec.SkipHeader {
			opts = append(opts, csvFile.WithHeader())
		}

		client, err := csvFile.NewClient(opts...)
		if err != nil {
			return &Client{}, err
		}
		return &Client{
			spec: spec,
			csv:  client,
		}, nil

	case FormatTypeJSON:
		client, err := jsonFile.NewClient()
		if err != nil {
			return &Client{}, err
		}
		return &Client{
			spec: spec,
			json: client,
		}, nil

	default:
		panic("unknown format " + spec.Format)
	}
}
