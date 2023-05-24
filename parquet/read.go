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
		cols[i] = reverseTransformArray(sc.Field(i).Type, rec.Column(i))
	}
	return array.NewRecord(sc, cols, -1)
}

func reverseTransformArray(dt arrow.DataType, col arrow.Array) arrow.Array {
	switch arr := col.(type) {
	case *array.String:
		return reverseTransformFromString(dt, arr)
	case *array.Timestamp:
		return reverseTransformTimestamp(dt.(*arrow.TimestampType), arr)
	case array.ListLike:
		values := reverseTransformArray(dt.(listLikeType).Elem(), arr.ListValues())
		res := array.NewListData(array.NewData(
			dt, arr.Len(),
			arr.Data().Buffers(),
			[]arrow.ArrayData{values.Data()},
			arr.NullN(), arr.Data().Offset(),
		))
		return res
	}

	if isUnsupportedType(dt) {
		return reverseTransformFromString(dt, col)
	}

	return col
}

func reverseTransformFromString(dt arrow.DataType, col arrow.Array) arrow.Array {
	builder := array.NewBuilder(memory.DefaultAllocator, dt)
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			builder.AppendNull()
			continue
		}
		if err := builder.AppendValueFromString(col.ValueStr(i)); err != nil {
			panic(fmt.Errorf("failed to append string %q value: %w", col.ValueStr(i), err))
		}
	}
	return builder.NewArray()
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
