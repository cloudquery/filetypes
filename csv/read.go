package csv

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

func (cl *Client) Read(r types.ReaderAtSeeker, table *schema.Table, res chan<- arrow.RecordBatch) error {
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
		rec := reader.RecordBatch()
		castRec, err := castFromString(rec, arrowSchema)
		if err != nil {
			return fmt.Errorf("failed to cast extension types: %w", err)
		}
		res <- castRec
	}
	return nil
}

// castFromString casts extension columns to string.
func castFromString(rec arrow.RecordBatch, arrowSchema *arrow.Schema) (arrow.RecordBatch, error) {
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
	return array.NewRecordBatch(arrowSchema, cols, rec.NumRows()), nil
}
