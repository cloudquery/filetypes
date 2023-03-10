package filetypes

import (
	"io"

	"github.com/cloudquery/plugin-sdk/schema"
)

func (cl *Client) WriteTableBatchFile(w io.Writer, table *schema.Table, resources [][]any) error {
	switch cl.spec.Format {
	case FormatTypeCSV:
		if err := cl.csv.WriteTableBatch(w, table, resources); err != nil {
			return err
		}
	case FormatTypeJSON:
		if err := cl.json.WriteTableBatch(w, table, resources); err != nil {
			return err
		}
	case FormatTypeParquet:
		if err := cl.parquet.WriteTableBatch(w, table, resources); err != nil {
			return err
		}
	default:
		panic("unknown format " + cl.spec.Format)
	}
	return nil
}
