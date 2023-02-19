package parquet

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func (*Client) WriteTableBatch(w io.Writer, table *schema.Table, resources [][]any) error {
	pw, err := writer.NewJSONWriterFromWriter(makeSchema(table.Name, table.Columns), w, 2)
	if err != nil {
		return fmt.Errorf("can't create parquet writer: %w", err)
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for i := range resources {
		rec := make(map[string]any, len(table.Columns))
		for j := range table.Columns {
			rec[table.Columns[j].Name] = resources[i][j]
		}
		b, _ := json.Marshal(rec)
		if err := pw.Write(b); err != nil {
			return err
		}
	}

	return pw.WriteStop()
}
