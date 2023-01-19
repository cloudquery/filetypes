package filetypes

import (
	"io"

	"github.com/cloudquery/plugin-sdk/schema"
)

func (cl *Client) Read(r io.Reader, table *schema.Table, sourceName string, res chan<- []any) error {
	switch cl.spec.Format {
	case FormatTypeCSV:
		if err := cl.csv.Read(r, table, sourceName, res); err != nil {
			return err
		}
	case FormatTypeJSON:
		if err := cl.json.Read(r, table, sourceName, res); err != nil {
			return err
		}
	default:
		panic("unknown format " + cl.spec.Format)
	}
	return nil
}

func (c *Client) ReverseTransformValues(table *schema.Table, values []any) (schema.CQTypes, error) {
	switch c.spec.Format {
	case FormatTypeCSV:
		return c.csvReverseTransformer.ReverseTransformValues(table, values)
	case FormatTypeJSON:
		return c.jsonReverseTransformer.ReverseTransformValues(table, values)
	default:
		panic("unknown format " + c.spec.Format)
	}
}
