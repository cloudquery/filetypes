package csv

import (
	// "encoding/csv"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/csv"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/cloudquery/plugin-sdk/v3/schema"
)

func (cl *Client) WriteTableBatch(w io.Writer, table *schema.Table, records []arrow.Record) error {
	newSchema := convertSchema(table.ToArrowSchema())
	writer := csv.NewWriter(w, newSchema,
		csv.WithComma(cl.Delimiter),
		csv.WithHeader(cl.IncludeHeaders),
		csv.WithNullWriter(""),
	)
	for _, record := range records {
		castRec, err := castToString(record)
		if err != nil {
			return fmt.Errorf("failed to cast to string: %w", err)
		}

		if err := writer.Write(castRec); err != nil {
			return fmt.Errorf("failed to write record to csv: %w", err)
		}
		if err := writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush csv writer: %w", err)
		}
	}
	return nil
}

func convertSchema(sch *arrow.Schema) *arrow.Schema {
	oldFields := sch.Fields()
	fields := make([]arrow.Field, len(oldFields))
	copy(fields, oldFields)
	for i, f := range fields {
		if !isTypeSupported(f.Type) {
			fields[i].Type = arrow.BinaryTypes.String
		}
	}

	md := sch.Metadata()
	newSchema := arrow.NewSchema(fields, &md)
	return newSchema
}

func isTypeSupported(t arrow.DataType) bool {
	// list from arrow/csv/common.go
	switch t.(type) {
	case *arrow.BooleanType:
	case *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type:
	case *arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type:
	case *arrow.Float32Type, *arrow.Float64Type:
	case *arrow.StringType:
	case *arrow.TimestampType:
	case *arrow.Date32Type, *arrow.Date64Type:
	case *arrow.Decimal128Type, *arrow.Decimal256Type:
	case *arrow.ListType:
	case *arrow.BinaryType:
	case arrow.ExtensionType:
		return true
	}

	/*
		switch t.ID() {
		case arrow.BOOL,
			arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
			arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
			arrow.FLOAT32, arrow.FLOAT64,
			arrow.STRING,
			arrow.TIMESTAMP,
			arrow.DATE32, arrow.DATE64,
			arrow.BINARY, arrow.EXTENSION:
			return true
		case arrow.LIST:
			return isTypeSupported(t.(*arrow.ListType).Elem())
		}
	*/
	return false
}

// castToString casts extension columns or unsupported columns to string. It does not release the original record.
func castToString(rec arrow.Record) (arrow.Record, error) {
	newSchema := convertSchema(rec.Schema())
	cols := make([]arrow.Array, rec.NumCols())
	for c := 0; c < int(rec.NumCols()); c++ {
		col := rec.Column(c)
		if isTypeSupported(col.DataType()) {
			cols[c] = col
			continue
		}

		sb := array.NewStringBuilder(memory.DefaultAllocator)
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				sb.AppendNull()
				continue
			}
			sb.Append(col.ValueStr(i))
		}
		cols[c] = sb.NewArray()
	}
	return array.NewRecord(newSchema, cols, rec.NumRows()), nil
}
