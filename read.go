package filetypes

import (
	"os"

	"github.com/apache/arrow/go/v12/arrow"
)

func (cl *Client) Read(f *os.File, sc *arrow.Schema, sourceName string, res chan<- arrow.Record) error {
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
