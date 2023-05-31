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
		if err := fw.Write(transformRecord(newSchema, rec)); err != nil {
			return err
		}
	}
	return fw.Close()
}

func convertSchema(sc *arrow.Schema) *arrow.Schema {
	fields := convertFieldTypes(sc.Fields()...)
	md := arrow.MetadataFrom(sc.Metadata().ToMap())
	newSchema := arrow.NewSchema(fields, &md)
	return newSchema
}

func convertFieldTypes(fields ...arrow.Field) []arrow.Field {
	res := make([]arrow.Field, len(fields))
	for i, field := range fields {
		res[i] = field
		res[i].Type = transformDataType(field.Type)
	}
	return res
}

func transformDataType(t arrow.DataType) arrow.DataType {
	switch dt := t.(type) {
	case *arrow.DurationType,
		*arrow.DayTimeIntervalType,
		*arrow.MonthDayNanoIntervalType,
		*arrow.MonthIntervalType: // unsupported in pqarrow
		return arrow.BinaryTypes.String

	case *arrow.LargeBinaryType,
		*arrow.LargeListType,
		*arrow.LargeStringType: // not yet implemented in arrow
		return arrow.BinaryTypes.String

	case *types.JSONType,
		*types.MACType,
		*types.InetType,
		*types.UUIDType:
		return arrow.BinaryTypes.String

	case *arrow.StructType:
		return arrow.StructOf(convertFieldTypes(dt.Fields()...)...)

	case *arrow.MapType:
		return arrow.MapOf(transformDataType(dt.KeyType()), transformDataType(dt.ItemType()))

	case listLikeType:
		return arrow.ListOf(transformDataType(dt.Elem()))
	default:
		return t
	}
}

// transformRecord casts extension columns or unsupported columns to string. It does not release the original record.
func transformRecord(sc *arrow.Schema, rec arrow.Record) arrow.Record {
	cols := make([]arrow.Array, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		cols[i] = transformArray(rec.Column(i))
	}
	return array.NewRecord(sc, cols, rec.NumRows())
}

func transformArray(arr arrow.Array) arrow.Array {
	if arrow.TypeEqual(arrow.BinaryTypes.String, transformDataType(arr.DataType())) {
		return transformToString(arr)
	}

	switch arr := arr.(type) {
	case *array.Struct:
		dt := arr.DataType().(*arrow.StructType)
		children := make([]arrow.ArrayData, arr.NumField())
		names := make([]string, arr.NumField())
		for i := range children {
			children[i] = transformArray(arr.Field(i)).Data()
			names[i] = dt.Field(i).Name
		}

		return array.NewStructData(array.NewData(
			transformDataType(dt), arr.Len(),
			arr.Data().Buffers(),
			children,
			arr.NullN(), arr.Data().Offset(),
		))

	case array.ListLike:
		return array.MakeFromData(array.NewData(
			transformDataType(arr.DataType()), arr.Len(),
			arr.Data().Buffers(),
			[]arrow.ArrayData{transformArray(arr.ListValues()).Data()},
			arr.NullN(), arr.Data().Offset(),
		))

	default:
		return arr
	}
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

// got=struct<binary: binary, boolean: bool, date32: date32, date64: timestamp[ms, tz=UTC], daytimeinterval: utf8, duration_ms: utf8, duration_ns: utf8, duration_s: utf8, duration_us: utf8, float32: float32, float64: float64, inet: utf8, int16: int16, int32: int32, int64: int64, int8: int8, largebinary: utf8, largestring: utf8, mac: utf8, monthdaynanointerval: utf8, monthinterval: utf8, string: utf8, time32ms: time32[ms], time32s: time32[ms], time64ns: time64[ns], time64us: time64[us], timestamp_ms: timestamp[ms, tz=UTC], timestamp_ns: timestamp[ns, tz=UTC], timestamp_s: timestamp[ms, tz=UTC], timestamp_us: timestamp[us, tz=UTC], uint16: uint16, uint32: uint32, uint64: uint64, uint8: uint8, uuid: utf8, decimal: decimal(19, 10), json: utf8, json_array: utf8>,
//want=struct<binary: binary, boolean: bool, date32: date32, date64: date64, daytimeinterval: utf8, duration_ms: utf8, duration_ns: utf8, duration_s: utf8, duration_us: utf8, float32: float32, float64: float64, inet: utf8, int16: int16, int32: int32, int64: int64, int8: int8, largebinary: utf8, largestring: utf8, mac: utf8, monthdaynanointerval: utf8, monthinterval: utf8, string: utf8, time32ms: time32[ms], time32s: time32[s], time64ns: time64[ns], time64us: time64[us], timestamp_ms: timestamp[ms, tz=UTC], timestamp_ns: timestamp[ns, tz=UTC], timestamp_s: timestamp[s, tz=UTC], timestamp_us: timestamp[us, tz=UTC], uint16: uint16, uint32: uint32, uint64: uint64, uint8: uint8, uuid: utf8, decimal: decimal(19, 10), json: utf8, json_array: utf8>
