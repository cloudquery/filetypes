package parquet

import (
	"bytes"
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
	"github.com/goccy/go-json"
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
func transformRecord(rec arrow.Record) arrow.Record {
	newSchema := convertSchema(rec.Schema())
	cols := make([]arrow.Array, rec.NumCols())
	for c := 0; c < int(rec.NumCols()); c++ {
		cols[c] = transformArray(rec.Column(c))
	}
	return array.NewRecord(newSchema, cols, rec.NumRows())
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
		newType := transformSchemaField(arr.DataType())

		list := arr.(array.ListLike)
		bldr := array.NewBuilder(memory.DefaultAllocator, newType).(array.ListLikeBuilder)
		vb := bldr.ValueBuilder()
		for i := 0; i < list.Len(); i++ {
			if list.IsNull(i) {
				bldr.AppendNull()
				continue
			}
			bldr.Append(true)
			start, end := list.ValueOffsets(i)

			slc := array.NewSlice(list.ListValues(), start, end)
			fillInArr(vb, slc)
			//fillInArr(bldr, slc)
		}

		return bldr.NewArray()

	case isUnsupportedType(arr.DataType()):
		return transformToStringArray(arr)
	default:
		return arr
	}
}

func fillInArr(vb array.Builder, arr arrow.Array) {
	//func fillInArr(bldr array.ListLikeBuilder, arr arrow.Array) {
	//	bldr.Append(true)

	//vb := bldr.ValueBuilder()
	useValueStr := transformSchemaField(arr.DataType()).ID() == arrow.STRING

	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			vb.AppendNull()
			continue
		}

		if arrow.IsListLike(arr.DataType().ID()) {
			list := arr.(array.ListLike)
			start, end := list.ValueOffsets(i)
			slc := array.NewSlice(list.ListValues(), start, end)
			//fillInArr(bldr, slc)
			fillInArr(vb, slc)
			return
		}

		var val any

		if useValueStr {
			val = arr.ValueStr(i)
		} else {
			val = arr.GetOneForMarshal(i)
		}

		b, err := json.MarshalWithOption(val, json.DisableHTMLEscape())
		if err != nil {
			panic(err)
		}
		fmt.Println("marshalled", string(b))

		dec := json.NewDecoder(bytes.NewReader(b))
		if err := vb.UnmarshalOne(dec); err != nil {
			panic(err)
		}
	}
}

func transformToStringArray(arr arrow.Array) arrow.Array {
	bldr := array.NewStringBuilder(memory.DefaultAllocator)
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			bldr.AppendNull()
			continue
		}

		bldr.Append(arr.ValueStr(i))
	}
	return bldr.NewArray()
}
