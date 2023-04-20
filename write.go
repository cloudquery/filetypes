package filetypes

import (
	"io"

	"github.com/apache/arrow/go/v12/arrow"
)

func (cl *Client) WriteTableBatchFile(w io.Writer, arrowSchema *arrow.Schema, records []arrow.Record) error {
	switch cl.spec.Format {
	case FormatTypeCSV:
		if err := cl.csv.WriteTableBatch(w, arrowSchema, records); err != nil {
			return err
		}
	case FormatTypeJSON:
		if err := cl.json.WriteTableBatch(w, arrowSchema, records); err != nil {
			return err
		}
	case FormatTypeParquet:
		if err := cl.parquet.WriteTableBatch(w, arrowSchema, records); err != nil {
			return err
		}
	default:
		panic("unknown format " + cl.spec.Format)
	}
	return nil
}
