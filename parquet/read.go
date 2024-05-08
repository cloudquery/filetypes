package parquet

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

func (*Client) Read(f types.ReaderAtSeeker, table *schema.Table, res chan<- arrow.Record) error {
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
		res <- reverseTransformRecord(sc, rr.Record())
	}
	if rr.Err() != nil && rr.Err() != io.EOF {
		return fmt.Errorf("failed to read parquet record: %w", rr.Err())
	}

	return nil
}

func reverseTransformRecord(sc *arrow.Schema, rec arrow.Record) arrow.Record {
	cols := make([]arrow.Array, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		cols[i] = reverseTransformArray(sc.Field(i).Type, rec.Column(i))
	}
	return array.NewRecord(sc, cols, -1)
}

func reverseTransformArray(dt arrow.DataType, arr arrow.Array) arrow.Array {
	if arrow.TypeEqual(dt, arr.DataType()) {
		return arr
	}

	switch arr := arr.(type) {
	case *array.String:
		return reverseTransformFromString(dt, arr)
	case *array.Timestamp:
		return reverseTransformFromTimestamp(dt, arr)
	case *array.Time32:
		return reverseTransformTime32(dt.(*arrow.Time32Type), arr)
	case *array.Time64:
		return reverseTransformTime64(dt.(*arrow.Time64Type), arr)
	case *array.Date32:
		return reverseTransformFromDate32(dt, arr)
	case *array.Struct:
		dt := dt.(*arrow.StructType)
		children := make([]arrow.ArrayData, arr.NumField())
		for i := range children {
			// struct fields can be odd when read from parquet, but the data is intact
			child := array.MakeFromData(arr.Data().Children()[i])
			children[i] = reverseTransformArray(dt.Field(i).Type, child).Data()
		}

		return array.NewStructData(array.NewData(
			dt, arr.Len(),
			arr.Data().Buffers(),
			children,
			arr.NullN(),
			0, // we use 0 as offset for struct arrays, as the child arrays would already be sliced properly
		))

	case array.ListLike: // this also handles maps
		return array.MakeFromData(array.NewData(
			dt, arr.Len(),
			arr.Data().Buffers(),
			[]arrow.ArrayData{reverseTransformArray(dt.(arrow.ListLikeType).Elem(), arr.ListValues()).Data()},
			arr.NullN(),
			// we use data offset for list like as the `ListValues` can be a larger array (happens when slicing)
			arr.Data().Offset(),
		))

	default:
		panic(fmt.Errorf("unsupported conversion from %s to %s", arr.DataType(), dt))
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
