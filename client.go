package filetypes

import (
	csvfile "github.com/cloudquery/filetypes/v4/csv"
	jsonfile "github.com/cloudquery/filetypes/v4/json"
	"github.com/cloudquery/filetypes/v4/parquet"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/filetypes/v4/xlsx"
)

type Client struct {
	spec     *FileSpec
	filetype types.FileType
}

var (
	_ types.FileType = (*Client)(nil)
	_ types.FileType = (*csvfile.Client)(nil)
	_ types.FileType = (*jsonfile.Client)(nil)
	_ types.FileType = (*parquet.Client)(nil)
	_ types.FileType = (*xlsx.Client)(nil)
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
		opts := []csvfile.Options{
			csvfile.WithDelimiter([]rune(spec.csvSpec.Delimiter)[0]),
		}
		if !spec.csvSpec.SkipHeader {
			opts = append(opts, csvfile.WithHeader())
		}

		client, err = csvfile.NewClient(opts...)

	case FormatTypeJSON:
		client, err = jsonfile.NewClient()

	case FormatTypeParquet:
		client, err = parquet.NewClient(parquet.WithSpec(*spec.parquetSpec))

	case FormatTypeXLSX:
		client, err = xlsx.NewClient()

	default:
		// shouldn't be possible as Validate checks for type
		panic("unknown format " + spec.Format)
	}

	if err != nil {
		return nil, err
	}

	return &Client{spec: spec, filetype: client}, nil
}
