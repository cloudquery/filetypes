package filetypes

import (
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/cloudquery/plugin-sdk/v3/schema"
)

func (cl *Client) WriteTableBatchFile(w io.Writer, table *schema.Table, records []arrow.Record) error {
	switch cl.spec.Format {
	case FormatTypeCSV:
		if err := cl.csv.WriteTableBatch(w, table, records); err != nil {
			return err
		}
	case FormatTypeJSON:
		if err := cl.json.WriteTableBatch(w, table, records); err != nil {
			return err
		}
	case FormatTypeParquet:
		if err := cl.parquet.WriteTableBatch(w, table, records); err != nil {
			return err
		}
	default:
		panic("unknown format " + cl.spec.Format)
	}
	return nil
}
