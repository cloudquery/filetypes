package parquet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func (*Client) WriteTableBatch(w io.Writer, table *schema.Table, records []arrow.Record) error {
	// TODO: use arrow's parquet writer
	pw, err := writer.NewJSONWriterFromWriter(makeSchema(table.Name, table.Columns), w, 2)
	if err != nil {
		return fmt.Errorf("can't create parquet writer: %w", err)
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, rec := range records {
		for r := int64(0); r < rec.NumRows(); r++ {
			m := rec.NewSlice(r, r+1)
			b, err := json.Marshal(m)
			if err != nil {
				return fmt.Errorf("can't marshal record: %w", err)
			}
			// hacky, but we don't have a better way to get json for a single row
			b = bytes.TrimPrefix(b, []byte("["))
			b = bytes.TrimSuffix(b, []byte("]"))
			if err := pw.Write(b); err != nil {
				return err
			}
		}
		pw.Flush(true)
	}

	return pw.WriteStop()
}
