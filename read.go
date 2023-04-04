package filetypes

import (
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/cloudquery/plugin-sdk/schema"
)

func (cl *Client) Read(r io.Reader, table *schema.Table, sourceName string, res chan<- arrow.Record) error {
	switch cl.spec.Format {
	case FormatTypeCSV:
		if err := cl.csv.Read(r, table, sourceName, res); err != nil {
			return err
		}
	case FormatTypeJSON:
		if err := cl.json.Read(r, table, sourceName, res); err != nil {
			return err
		}
	case FormatTypeParquet:
		if err := cl.parquet.Read(r, table, sourceName, res); err != nil {
			return err
		}
	default:
		panic("unknown format " + cl.spec.Format)
	}
	return nil
}

func (cl *Client) ReverseTransformValues(table *schema.Table, values []any) (schema.CQTypes, error) {
	switch cl.spec.Format {
	case FormatTypeCSV:
		return cl.csvReverseTransformer.ReverseTransformValues(table, values)
	case FormatTypeJSON:
		return cl.jsonReverseTransformer.ReverseTransformValues(table, values)
	case FormatTypeParquet:
		return cl.parquetReverseTransformer.ReverseTransformValues(table, values)
	default:
		panic("unknown format " + cl.spec.Format)
	}
}
