package parquet

import (
	"bytes"
	"fmt"
	"io"

	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/xitongsys/parquet-go/reader"
)

func (*Client) Read(f io.Reader, table *schema.Table, sourceName string, res chan<- []any) error {
	sourceNameIndex := int64(table.Columns.Index(schema.CqSourceNameColumn.Name))
	if sourceNameIndex == -1 {
		return fmt.Errorf("could not find column %s in table %s", schema.CqSourceNameColumn.Name, table.Name)
	}

	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, f); err != nil {
		return err
	}

	s := makeSchema(table.Columns)
	r, err := reader.NewParquetReader(newPQReader(buf.Bytes()), s, 2)
	if err != nil {
		return fmt.Errorf("can't create parquet reader: %w", err)
	}
	defer r.ReadStop()

	for row := int64(0); row < r.GetNumRows(); row++ {
		record := make([]any, len(table.Columns))
		for col := 0; col < len(table.Columns); col++ {
			vals, _, _, err := r.ReadColumnByIndex(int64(col), 1)
			if err != nil {
				return err
			}
			if len(vals) == 1 {
				record[col] = vals[0]
			} else {
				record[col] = vals
			}
		}

		if record[sourceNameIndex] == sourceName {
			res <- record
		}
	}

	return nil
}
