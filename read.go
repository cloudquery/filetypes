package filetypes

import (
	"bytes"
	"compress/gzip"
	"io"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

type ReaderAtSeeker interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

func (cl *Client) Read(f ReaderAtSeeker, table *schema.Table, res chan<- arrow.Record) error {
	if cl.spec.Compression == CompressionTypeGZip {
		rr, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer rr.Close()
		b, err := io.ReadAll(rr)
		if err != nil {
			return err
		}
		f = bytes.NewReader(b)
	}

	switch cl.spec.Format {
	case FormatTypeCSV:
		if err := cl.csv.Read(f, table, res); err != nil {
			return err
		}
	case FormatTypeJSON:
		if err := cl.json.Read(f, table, res); err != nil {
			return err
		}
	case FormatTypeParquet:
		if err := cl.parquet.Read(f, table, res); err != nil {
			return err
		}
	default:
		panic("unknown format " + cl.spec.Format)
	}
	return nil
}
