package parquet

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet/file"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"
	"github.com/cloudquery/plugin-sdk/v3/schema"
	"github.com/cloudquery/plugin-sdk/v3/types"
)

type ReaderAtSeeker interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

func (*Client) Read(f ReaderAtSeeker, table *schema.Table, _ string, res chan<- arrow.Record) error {
	ctx := context.Background()
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		return fmt.Errorf("failed to create new parquet reader: %w", err)
	}
	arrProps := pqarrow.ArrowReadProperties{
		Parallel:  false,
		BatchSize: 1024,
	}
	fr, err := pqarrow.NewFileReader(rdr, arrProps, memory.DefaultAllocator)
	if err != nil {
		return fmt.Errorf("failed to create new parquet file reader: %w", err)
	}
	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to get parquet record reader: %w", err)
	}

	arrowSchema := table.ToArrowSchema()
	for rr.Next() {
		rec := rr.Record()
		newRecs := convertToSingleRowRecords(arrowSchema, rec)
		for _, r := range newRecs {
			res <- r
		}
	}
	if rr.Err() != nil && rr.Err() != io.EOF {
		return fmt.Errorf("failed to read parquet record: %w", rr.Err())
	}

	return nil
}

func convertToSingleRowRecords(sc *arrow.Schema, rec arrow.Record) []arrow.Record {
	records := make([]arrow.Record, rec.NumRows())
	for i := int64(0); i < rec.NumRows(); i++ {
		records[i] = reverseTransformRecord(sc, rec.NewSlice(i, i+1))
	}
	return records
}

func reverseTransformRecord(sc *arrow.Schema, rec arrow.Record) arrow.Record {
	cols := make([]arrow.Array, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		cols[i] = reverseTransformArray(sc.Field(i), rec.Column(i))
	}
	return array.NewRecord(sc, cols, -1)
}

func reverseTransformArray(f arrow.Field, col arrow.Array) arrow.Array {
	dt := f.Type
	switch {
	case arrow.TypeEqual(dt, types.ExtensionTypes.UUID):
		return reverseTransformUUID(col.(*array.String))
	case arrow.TypeEqual(dt, types.ExtensionTypes.Inet):
		return reverseTransformInet(col.(*array.String))
	case arrow.TypeEqual(dt, types.ExtensionTypes.MAC):
		return reverseTransformMAC(col.(*array.String))
	case arrow.TypeEqual(dt, types.ExtensionTypes.JSON):
		return reverseTransformJSON(col.(*array.String))
	case arrow.TypeEqual(col.DataType(), arrow.FixedWidthTypes.Timestamp_ms):
		return reverseTransformTimestamp(dt.(*arrow.TimestampType), col.(*array.Timestamp))
	case dt.ID() == arrow.STRUCT:
		return reverseTransformStruct(dt.(*arrow.StructType), col.(*array.String))
	case arrow.IsListLike(dt.ID()) && dt.(*arrow.ListType).Elem().ID() == arrow.EXTENSION:
		child := reverseTransformArray(
			arrow.Field{
				Type: dt.(*arrow.ListType).Elem(),
				Name: "list<ext:" + dt.(*arrow.ListType).Elem().Name() + ">",
			},
			col.(*array.List).ListValues(),
		)
		fmt.Println("counts", col.Len(), child.Len(), col.NullN(), child.NullN()) //  1 3 0 1

		return array.NewExtensionData(array.NewData(dt.(*arrow.ListType).Elem(), child.Len(), child.Data().Buffers(), []arrow.ArrayData{child.Data()}, child.NullN(), child.Data().Offset()))

	case arrow.IsListLike(dt.ID()):
		child := reverseTransformArray(
			arrow.Field{
				Type: dt.(*arrow.ListType).Elem(),
				Name: "list<" + dt.(*arrow.ListType).Elem().Name() + ">",
			},
			col.(*array.List).ListValues(),
		)

		return array.NewListData(array.NewData(dt, col.Len(), col.Data().Buffers(), []arrow.ArrayData{child.Data()}, col.NullN(), col.Data().Offset()))
	case isUnsupportedType(dt):
		sb := array.NewBuilder(memory.DefaultAllocator, dt)
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				sb.AppendNull()
				continue
			}
			if err := sb.AppendValueFromString(col.ValueStr(i)); err != nil {
				panic(fmt.Errorf("failed to AppendValueFromString col %v: %w", f.Name, err))
			}
		}
		return sb.NewArray()

	default:
		return col
	}
}

func reverseTransformStruct(dt *arrow.StructType, col *array.String) arrow.Array {
	bldr := array.NewStructBuilder(memory.DefaultAllocator, dt)
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			bldr.AppendNull()
		} else {
			if err := bldr.AppendValueFromString(col.Value(i)); err != nil {
				panic(fmt.Errorf("failed to append json %s value: %w", col.Value(i), err))
			}
		}
	}

	return bldr.NewArray()
}

func reverseTransformJSON(col *array.String) arrow.Array {
	bldr := types.NewJSONBuilder(array.NewExtensionBuilder(memory.DefaultAllocator, types.ExtensionTypes.JSON))
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			bldr.AppendNull()
		} else {
			if err := bldr.AppendValueFromString(col.Value(i)); err != nil {
				panic(fmt.Errorf("failed to append json %s value: %w", col.Value(i), err))
			}
		}
	}

	return bldr.NewArray()
}

func reverseTransformMAC(col *array.String) arrow.Array {
	bldr := types.NewMACBuilder(array.NewExtensionBuilder(memory.DefaultAllocator, types.ExtensionTypes.MAC))
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			bldr.AppendNull()
		} else {
			if err := bldr.AppendValueFromString(col.Value(i)); err != nil {
				panic(err)
			}
		}
	}

	return bldr.NewMACArray()
}

func reverseTransformInet(col *array.String) arrow.Array {
	bldr := types.NewInetBuilder(array.NewExtensionBuilder(memory.DefaultAllocator, types.ExtensionTypes.Inet))
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			bldr.AppendNull()
		} else {
			if err := bldr.AppendValueFromString(col.Value(i)); err != nil {
				panic(err)
			}
		}
	}

	return bldr.NewInetArray()
}

func reverseTransformUUID(col *array.String) arrow.Array {
	bldr := types.NewUUIDBuilder(array.NewExtensionBuilder(memory.DefaultAllocator, types.ExtensionTypes.UUID))
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			bldr.AppendNull()
			continue
		}
		fmt.Println("uuid value", col.Value(i), col.ValueStr(i), col.IsValid(i), col.IsNull(i))

		if err := bldr.AppendValueFromString(col.Value(i)); err != nil {
			panic(err)
		}
	}

	return bldr.NewUUIDArray()
}

func reverseTransformTimestamp(dtype *arrow.TimestampType, col *array.Timestamp) arrow.Array {
	bldr := array.NewTimestampBuilder(memory.DefaultAllocator, dtype)
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			bldr.AppendNull()
		} else {
			t := col.Value(i).ToTime(col.DataType().(*arrow.TimestampType).Unit)
			switch dtype.Unit {
			case arrow.Second:
				bldr.Append(arrow.Timestamp(t.Unix()))
			case arrow.Millisecond:
				bldr.Append(arrow.Timestamp(t.UnixMilli()))
			case arrow.Microsecond:
				bldr.Append(arrow.Timestamp(t.UnixMicro()))
			case arrow.Nanosecond:
				bldr.Append(arrow.Timestamp(t.UnixNano()))
			default:
				panic(fmt.Errorf("unsupported timestamp unit: %s", dtype.Unit))
			}
		}
	}
	return bldr.NewTimestampArray()
}
