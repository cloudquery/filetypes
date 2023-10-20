package filetypes

import (
	csvFile "github.com/cloudquery/filetypes/v4/csv"
	jsonFile "github.com/cloudquery/filetypes/v4/json"
	"github.com/cloudquery/filetypes/v4/parquet"
	"github.com/cloudquery/filetypes/v4/types"
)

type Client struct {
	spec     *FileSpec
	filetype types.FileType
}

var (
	_ types.FileType = (*Client)(nil)
	_ types.FileType = (*csvFile.Client)(nil)
	_ types.FileType = (*jsonFile.Client)(nil)
	_ types.FileType = (*parquet.Client)(nil)
)

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

	var client types.FileType
	switch spec.Format {
	case FormatTypeCSV:
		opts := []csvFile.Options{
			csvFile.WithDelimiter([]rune(spec.csvSpec.Delimiter)[0]),
		}
		if !spec.csvSpec.SkipHeader {
			opts = append(opts, csvFile.WithHeader())
		}

		client, err = csvFile.NewClient(opts...)

	case FormatTypeJSON:
		client, err = jsonFile.NewClient()

	case FormatTypeParquet:
		client, err = parquet.NewClient(parquet.WithSpec(*spec.parquetSpec))

	default:
		panic("unknown format " + spec.Format)
	}
	if err != nil {
		return nil, err
	}
	return &Client{spec: spec, filetype: client}, nil
}
