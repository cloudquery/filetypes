package filetypes

import (
	"io"

	"github.com/apache/arrow/go/v12/arrow"
)

type ReaderAtSeeker interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

func (cl *Client) Read(f ReaderAtSeeker, sc *arrow.Schema, sourceName string, res chan<- arrow.Record) error {
	switch cl.spec.Format {
	case FormatTypeCSV:
		if err := cl.csv.Read(f, sc, sourceName, res); err != nil {
			return err
		}
	case FormatTypeJSON:
		if err := cl.json.Read(f, sc, sourceName, res); err != nil {
			return err
		}
	case FormatTypeParquet:
		if err := cl.parquet.Read(f, sc, sourceName, res); err != nil {
			return err
		}
	default:
		panic("unknown format " + cl.spec.Format)
	}
	return nil
}
