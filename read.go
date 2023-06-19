package filetypes

import (
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

type ReaderAtSeeker interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

func (cl *Client) Read(f ReaderAtSeeker, table *schema.Table, sourceName string, res chan<- arrow.Record) error {
	switch cl.spec.Format {
	case FormatTypeCSV:
		if err := cl.csv.Read(f, table, sourceName, res); err != nil {
			return err
		}
	case FormatTypeJSON:
		if err := cl.json.Read(f, table, sourceName, res); err != nil {
			return err
		}
	case FormatTypeParquet:
		if err := cl.parquet.Read(f, table, sourceName, res); err != nil {
			return err
		}
	default:
		panic("unknown format " + cl.spec.Format)
	}
	return nil
}
