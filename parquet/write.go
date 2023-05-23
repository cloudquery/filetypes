package parquet

import (
	"fmt"
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"
	"github.com/cloudquery/plugin-sdk/v3/schema"
	"github.com/cloudquery/plugin-sdk/v3/types"
)

func (c *Client) WriteTableBatch(w io.Writer, table *schema.Table, records []arrow.Record) error {
	props := parquet.NewWriterProperties(
		parquet.WithMaxRowGroupLength(128*1024*1024), // 128M
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	arrprops := pqarrow.DefaultWriterProps()
	newSchema := convertSchema(table.ToArrowSchema())
	fw, err := pqarrow.NewFileWriter(newSchema, w, props, arrprops)
	if err != nil {
		return err
	}
	for _, rec := range records {
		err := c.writeRecord(rec, fw)
		if err != nil {
			return err
		}
	}
	return fw.Close()
}

func (*Client) writeRecord(rec arrow.Record, fw *pqarrow.FileWriter) error {
	castRec, err := transformRecord(rec)
	if err != nil {
		return fmt.Errorf("failed to transform record: %w", err)
	}
	return fw.Write(castRec)
}

func convertSchema(sch *arrow.Schema) *arrow.Schema {
	oldFields := sch.Fields()
	fields := make([]arrow.Field, len(oldFields))
	for i := range fields {
		fields[i].Type = transformSchemaField(oldFields[i].Type)
	}

	md := sch.Metadata()
	newSchema := arrow.NewSchema(fields, &md)
	return newSchema
}

func transformSchemaField(t arrow.DataType) arrow.DataType {
	switch {
	case t.ID() == arrow.EXTENSION:
		return arrow.BinaryTypes.String
	case arrow.IsListLike(t.ID()):
		ct := t.(*arrow.ListType).Elem()
		return arrow.ListOf(transformSchemaField(ct))
	default:
		if isUnsupportedType(t) {
			return arrow.BinaryTypes.String
		}
		return t
	}
}

func isUnsupportedType(t arrow.DataType) bool {
	switch dt := t.(type) {
	case *arrow.DayTimeIntervalType, *arrow.DurationType, *arrow.MonthDayNanoIntervalType, *arrow.MonthIntervalType: // unsupported in pqarrow
		return true
	case *arrow.LargeBinaryType, *arrow.LargeListType, *arrow.LargeStringType: // not yet implemented in arrow
		return true
	case *arrow.StructType:
		for _, f := range dt.Fields() {
			if isUnsupportedType(f.Type) {
				return true
			}
		}
	}
	if arrow.IsListLike(t.ID()) {
		return isUnsupportedType(t.(*arrow.ListType).Elem())
	}
	return false
}

// transformRecord casts extension columns or unsupported columns to string. It does not release the original record.
func transformRecord(rec arrow.Record) (arrow.Record, error) {
	newSchema := convertSchema(rec.Schema())
	cols := make([]arrow.Array, rec.NumCols())
	for c := 0; c < int(rec.NumCols()); c++ {
		cols[c] = transformArray(rec.Column(c))
	}
	return array.NewRecord(newSchema, cols, rec.NumRows()), nil
}

func transformArray(arr arrow.Array) arrow.Array {
	dt := arr.DataType()
	switch {
	case arrow.TypeEqual(dt, types.ExtensionTypes.UUID) ||
		arrow.TypeEqual(dt, types.ExtensionTypes.Inet) ||
		arrow.TypeEqual(dt, types.ExtensionTypes.MAC) ||
		arrow.TypeEqual(dt, types.ExtensionTypes.JSON) ||
		dt.ID() == arrow.STRUCT:
		return transformToStringArray(arr)
	case arrow.IsListLike(dt.ID()):
		child := transformArray(arr.(*array.List).ListValues()).Data()
		newType := arrow.ListOf(child.DataType())
		return array.NewListData(array.NewData(newType, arr.Len(), arr.Data().Buffers(), []arrow.ArrayData{child}, arr.NullN(), arr.Data().Offset()))
	case isUnsupportedType(arr.DataType()):
		return transformToStringArray(arr)
	default:
		return arr
	}
}

func transformToStringArray(arr arrow.Array) arrow.Array {
	bldr := array.NewStringBuilder(memory.DefaultAllocator)
	for i := 0; i < arr.Len(); i++ {
		if err := bldr.AppendValueFromString(arr.ValueStr(i)); err != nil {
			panic(err)
		}
	}
	return bldr.NewArray()
}
