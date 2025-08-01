package parquet

import (
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	ftypes "github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/cloudquery/plugin-sdk/v4/types"
)

type Handle struct {
	w *pqarrow.FileWriter
	s *arrow.Schema
}

var _ ftypes.Handle = (*Handle)(nil)

func (c *Client) WriteHeader(w io.Writer, t *schema.Table) (ftypes.Handle, error) {
	props := parquet.NewWriterProperties(
		parquet.WithMaxRowGroupLength(c.spec.GetMaxRowGroupLength()),
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithVersion(c.spec.GetVersion()),
		parquet.WithRootRepetition(c.spec.GetRootRepetition()),
	)
	arrprops := pqarrow.DefaultWriterProps()
	newSchema := convertSchema(t.ToArrowSchema())
	fw, err := pqarrow.NewFileWriter(newSchema, &nopCloseWriter{Writer: w}, props, arrprops)
	if err != nil {
		return nil, err
	}

	return &Handle{
		w: fw,
		s: newSchema,
	}, nil
}

func (h *Handle) WriteFooter() error {
	err := h.w.Close()
	h.w = nil
	return err
}

func (h *Handle) WriteContent(records []arrow.Record) error {
	for _, rec := range records {
		if err := h.w.WriteBuffered(transformRecord(h.s, rec)); err != nil {
			return err
		}
	}
	return nil
}

func convertSchema(sc *arrow.Schema) *arrow.Schema {
	md := arrow.MetadataFrom(sc.Metadata().ToMap())
	return arrow.NewSchema(convertFieldTypes(sc.Fields()), &md)
}

func convertFieldTypes(fields []arrow.Field) []arrow.Field {
	for i := range fields {
		fields[i].Type = transformDataType(fields[i].Type)
	}
	return fields
}

func transformDataType(t arrow.DataType) arrow.DataType {
	switch dt := t.(type) {
	case *arrow.DurationType,
		*arrow.DayTimeIntervalType,
		*arrow.MonthDayNanoIntervalType,
		*arrow.MonthIntervalType: // unsupported in pqarrow
		return arrow.BinaryTypes.String

	case *arrow.LargeBinaryType,
		*arrow.LargeStringType: // not yet implemented in arrow
		return arrow.BinaryTypes.String

	case *types.JSONType,
		*types.MACType,
		*types.InetType,
		*types.UUIDType:
		return arrow.BinaryTypes.String

	case *arrow.StructType:
		return arrow.StructOf(convertFieldTypes(dt.Fields())...)

	case *arrow.MapType:
		return arrow.MapOf(transformDataType(dt.KeyType()), transformDataType(dt.ItemType()))

	case arrow.ListLikeType:
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
			arr.NullN(),
			0, // we use 0 as offset for struct arrays, as the child arrays would already be sliced properly
		))

	case array.ListLike: // this also handles maps
		return array.MakeFromData(array.NewData(
			transformDataType(arr.DataType()), arr.Len(),
			arr.Data().Buffers(),
			[]arrow.ArrayData{transformArray(arr.ListValues()).Data()},
			arr.NullN(),
			// we use data offset for list like as the `ListValues` can be a larger array (happens when slicing)
			arr.Data().Offset(),
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

type nopCloseWriter struct {
	io.Writer
}

func (*nopCloseWriter) Close() error {
	return nil
}

var _ io.WriteCloser = (*nopCloseWriter)(nil)
