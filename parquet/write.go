package parquet

import (
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
)

func (*Client) WriteTableBatch(w io.Writer, arrowSchema *arrow.Schema, records []arrow.Record) error {
	props := parquet.NewWriterProperties()
	arrprops := pqarrow.DefaultWriterProps()
	fw, err := pqarrow.NewFileWriter(arrowSchema, w, props, arrprops)
	if err != nil {
		return err
	}
	for _, rec := range records {
		if err := fw.Write(rec); err != nil {
			return err
		}
	}
	return fw.Close()
}
