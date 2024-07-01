package csv

import (
	// "encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/csv"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

type Handle struct {
	w *csv.Writer
}

var _ types.Handle = (*Handle)(nil)

func (cl *Client) WriteHeader(w io.Writer, t *schema.Table) (types.Handle, error) {
	s := t.ToArrowSchema()
	newSchema := convertSchema(s)
	writer := csv.NewWriter(w, newSchema,
		csv.WithComma(cl.Delimiter),
		csv.WithHeader(cl.IncludeHeaders),
		csv.WithNullWriter(""),
	)

	return &Handle{
		w: writer,
	}, nil
}

func (h *Handle) WriteContent(records []arrow.Record) error {
	for _, record := range records {
		castRec := transformRecord(record)
		if err := h.w.Write(castRec); err != nil {
			return fmt.Errorf("failed to write record to csv: %w", err)
		}
	}

	if err := h.w.Flush(); err != nil {
		return fmt.Errorf("failed to flush csv writer: %w", err)
	}
	return nil
}

func (h *Handle) WriteFooter() error {
	return h.w.Flush()
}

func convertSchema(sch *arrow.Schema) *arrow.Schema {
	oldFields := sch.Fields()
	fields := make([]arrow.Field, len(oldFields))
	copy(fields, oldFields)
	for i, f := range fields {
		fields[i].Type = convertType(f.Type)
		fields[i].Metadata = stripCQExtensionMetadata(f.Metadata)
	}

	md := sch.Metadata()
	newSchema := arrow.NewSchema(fields, &md)
	return newSchema
}

func convertType(dt arrow.DataType) arrow.DataType {
	if typeSupported(dt) {
		return dt
	}
	switch dt := dt.(type) {
	case *arrow.MapType:
	case *arrow.FixedSizeListType:
		// not supported -> elem not supported
		field := dt.ElemField()
		field.Type = convertType(field.Type)
		return arrow.FixedSizeListOfField(dt.Len(), field)
	case arrow.ListLikeType:
		// not supported -> elem not supported
		field := dt.ElemField()
		field.Type = convertType(field.Type)
		return arrow.ListOfField(field)
	}
	return arrow.BinaryTypes.String
}

// typeSupported copied from arrow/csv/common.go
func typeSupported(dt arrow.DataType) bool {
	switch dt := dt.(type) {
	case *arrow.BooleanType:
	case *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type:
	case *arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type:
	case *arrow.Float16Type, *arrow.Float32Type, *arrow.Float64Type:
	case *arrow.StringType, *arrow.LargeStringType:
	case *arrow.TimestampType:
	case *arrow.Date32Type, *arrow.Date64Type:
	case *arrow.Decimal128Type, *arrow.Decimal256Type:
	case *arrow.MapType:
		return false
	case arrow.ListLikeType:
		return typeSupported(dt.Elem())
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.FixedSizeBinaryType:
	case arrow.ExtensionType:
	case *arrow.NullType:
	default:
		return false
	}
	return true
}

func transformRecord(rec arrow.Record) arrow.Record {
	sc := convertSchema(rec.Schema())
	if sc.Equal(rec.Schema()) {
		return rec
	}

	cols := make([]arrow.Array, rec.NumCols())
	for i, col := range rec.Columns() {
		cols[i] = transformArray(col, sc.Field(i).Type)
	}
	return array.NewRecord(sc, cols, rec.NumRows())
}

func transformArray(arr arrow.Array, dt arrow.DataType) arrow.Array {
	if arrow.TypeEqual(arr.DataType(), dt) {
		return arr
	}

	if listDT, ok := dt.(arrow.ListLikeType); ok {
		listArr := arr.(array.ListLike)
		return array.MakeFromData(array.NewData(
			listDT, listArr.Len(),
			listArr.Data().Buffers(),
			[]arrow.ArrayData{transformArray(listArr.ListValues(), listDT.Elem()).Data()},
			listArr.NullN(),
			// we use data offset for list like as the `ListValues` can be a larger array (happens when slicing)
			listArr.Data().Offset(),
		))
	}

	return transformArrayToString(arr)
}

func transformArrayToString(arr arrow.Array) *array.String {
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	builder.Reserve(arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			builder.AppendNull()
			continue
		}
		builder.Append(arr.ValueStr(i))
	}
	return builder.NewStringArray()
}

func stripCQExtensionMetadata(md arrow.Metadata) arrow.Metadata {
	m := md.ToMap()
	for k := range m {
		if strings.HasPrefix(k, "cq:extension:") {
			delete(m, k)
		}
	}
	return arrow.MetadataFrom(m)
}
