package csv

import (
	"fmt"
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/csv"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

func (cl *Client) Read(r io.Reader, table *schema.Table, res chan<- arrow.Record) error {
	arrowSchema := table.ToArrowSchema()
	newSchema := convertSchema(arrowSchema)
	reader := csv.NewReader(r, newSchema,
		csv.WithComma(cl.Delimiter),
		csv.WithHeader(cl.IncludeHeaders),
		csv.WithNullReader(true, ""),
	)
	for reader.Next() {
		if reader.Err() != nil {
			return reader.Err()
		}
		rec := reader.Record()
		castRec, err := castFromString(rec, arrowSchema)
		if err != nil {
			return fmt.Errorf("failed to cast extension types: %w", err)
		}
		res <- castRec
	}
	return nil
}

// castFromString casts extension columns to string.
func castFromString(rec arrow.Record, arrowSchema *arrow.Schema) (arrow.Record, error) {
	cols := make([]arrow.Array, rec.NumCols())
	for c, f := range arrowSchema.Fields() {
		col := rec.Column(c)
		if isTypeSupported(f.Type) {
			cols[c] = col
			continue
		}

		sb := array.NewBuilder(memory.DefaultAllocator, f.Type)
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				sb.AppendNull()
				continue
			}
			if err := sb.AppendValueFromString(col.ValueStr(i)); err != nil {
				return nil, fmt.Errorf("failed to AppendValueFromString col %v: %w", rec.ColumnName(c), err)
			}
		}
		cols[c] = sb.NewArray()
	}
	return array.NewRecord(arrowSchema, cols, rec.NumRows()), nil
}
