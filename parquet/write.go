package parquet

import (
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

func (*Client) WriteTableBatch(w io.Writer, table *schema.Table, records []arrow.Record) error {
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
		if err := fw.Write(transformRecord(rec)); err != nil {
			return err
		}
	}
	return fw.Close()
}

func convertSchema(sch *arrow.Schema) *arrow.Schema {
	oldFields := sch.Fields()
	fields := make([]arrow.Field, len(oldFields))
	for i := range fields {
		fields[i].Type = transformDataType(oldFields[i].Type)
	}

	md := sch.Metadata()
	newSchema := arrow.NewSchema(fields, &md)
	return newSchema
}

func transformDataType(t arrow.DataType) arrow.DataType {
	switch dt := t.(type) {
	case *types.JSONType,
		*types.MACType,
		*types.InetType,
		*types.UUIDType:
		return arrow.BinaryTypes.String
	case listLikeType:
		return arrow.ListOf(transformDataType(dt.Elem()))
	}

	if isUnsupportedType(t) {
		return arrow.BinaryTypes.String
	}

	return t
}

func isUnsupportedType(t arrow.DataType) bool {
	switch dt := t.(type) {
	case *arrow.DurationType,
		*arrow.DayTimeIntervalType,
		*arrow.MonthDayNanoIntervalType,
		*arrow.MonthIntervalType: // unsupported in pqarrow
		return true
	case *arrow.LargeBinaryType,
		*arrow.LargeListType,
		*arrow.LargeStringType: // not yet implemented in arrow
		return true
	case *arrow.StructType:
		for _, f := range dt.Fields() {
			if isUnsupportedType(f.Type) {
				return true
			}
		}
	case listLikeType:
		return isUnsupportedType(dt.Elem())
	}
	return false
}

// transformRecord casts extension columns or unsupported columns to string. It does not release the original record.
func transformRecord(rec arrow.Record) arrow.Record {
	newSchema := convertSchema(rec.Schema())
	cols := make([]arrow.Array, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		cols[i] = transformArray(rec.Column(i))
	}
	return array.NewRecord(newSchema, cols, rec.NumRows())
}

func transformArray(arr arrow.Array) arrow.Array {
	switch arr := arr.(type) {
	case *types.UUIDArray,
		*types.InetArray,
		*types.MACArray,
		*types.JSONArray,
		*array.Struct:
		return transformToString(arr)
	case array.ListLike:
		values := transformArray(arr.ListValues())
		return array.NewListData(array.NewData(
			transformDataType(arr.DataType()), arr.Len(),
			arr.Data().Buffers(),
			[]arrow.ArrayData{values.Data()},
			arr.NullN(), values.Data().Offset(),
		))
	}

	if isUnsupportedType(arr.DataType()) {
		return transformToString(arr)
	}

	return arr
}

func transformToString(arr arrow.Array) arrow.Array {
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			builder.AppendNull()
			continue
		}
		builder.Append(arr.ValueStr(i))
	}

	return builder.NewArray()
}
