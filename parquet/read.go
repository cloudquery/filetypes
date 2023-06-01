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

	sc := table.ToArrowSchema()
	for rr.Next() {
		rec := rr.Record()
		newRecs := convertToSingleRowRecords(sc, rec)
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
		cols[i] = reverseTransformArray(sc.Field(i).Type, rec.Column(i))
	}
	return array.NewRecord(sc, cols, -1)
}

func reverseTransformArray(dt arrow.DataType, arr arrow.Array) arrow.Array {
	switch arr := arr.(type) {
	case *array.String:
		return reverseTransformFromString(dt, arr)
	case *array.Timestamp:
		return reverseTransformFromTimestamp(dt, arr)
	case *array.Time32:
		return reverseTransformTime32(dt.(*arrow.Time32Type), arr)
	case *array.Time64:
		return reverseTransformTime64(dt.(*arrow.Time64Type), arr)
	case *array.Struct:
		return reverseTransformStruct(dt.(*arrow.StructType), arr)

	case array.ListLike:
		var child arrow.ArrayData
		switch dt := dt.(type) {
		case *arrow.MapType:
			child = reverseTransformArray(dt.ValueType(), arr.ListValues()).Data()
		case listLikeType:
			child = reverseTransformArray(dt.Elem(), arr.ListValues()).Data()
		default:
			panic("unsupported list like conv to " + dt.String())
		}
		return array.MakeFromData(array.NewData(
			dt, arr.Len(),
			arr.Data().Buffers(),
			[]arrow.ArrayData{child},
			arr.NullN(), arr.Data().Offset(),
		))

	default:
		return arr
	}
}

func reverseTransformFromString(dt arrow.DataType, arr arrow.Array) arrow.Array {
	builder := array.NewBuilder(memory.DefaultAllocator, dt)
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			builder.AppendNull()
			continue
		}
		if err := builder.AppendValueFromString(arr.ValueStr(i)); err != nil {
			panic(fmt.Errorf("failed to append string %q value: %w", arr.ValueStr(i), err))
		}
	}
	return builder.NewArray()
}

func reverseTransformStruct(dt *arrow.StructType, arr *array.Struct) *array.Struct {
	children := make([]arrow.Array, arr.NumField())
	names := make([]string, arr.NumField())
	for i := range children {
		children[i] = reverseTransformArray(dt.Field(i).Type, arr.Field(i))
		names[i] = dt.Field(i).Name
	}

	// structs are sometimes read oddly when the outer struct is nullable but the inner one isn't
	builder := array.NewStructBuilder(memory.DefaultAllocator, dt)

	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			builder.AppendNull()
			continue
		}

		builder.Append(true)
		for j, c := range children {
			if err := builder.FieldBuilder(j).AppendValueFromString(c.ValueStr(i)); err != nil {
				panic(err)
			}
		}
	}

	return builder.NewStructArray()
}
