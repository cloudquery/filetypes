package parquet

import (
	"fmt"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
)

func reverseTransformTimestamp(dt *arrow.TimestampType, arr *array.Timestamp) arrow.Array {
	builder := array.NewTimestampBuilder(memory.DefaultAllocator, dt)
	in, out := arr.DataType().(*arrow.TimestampType).Unit, dt.Unit

	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			builder.AppendNull()
			continue
		}

		builder.Append(arrow.Timestamp(arrow.ConvertTimestampValue(in, out, int64(arr.Value(i)))))
	}

	return builder.NewArray()
}

func reverseTransformFromTimestamp(dt arrow.DataType, arr *array.Timestamp) arrow.Array {
	toTime, err := arr.DataType().(*arrow.TimestampType).GetToTimeFunc()
	if err != nil {
		panic(fmt.Errorf("failed GetToTimeFunc: %w", err))
	}

	switch dt := dt.(type) {
	case *arrow.Date32Type:
		return reverseTransformDate32(arr, toTime)
	case *arrow.Date64Type:
		return reverseTransformDate64(arr, toTime)
	case *arrow.TimestampType:
		return reverseTransformTimestamp(dt, arr)
	default:
		return reverseTransformFromString(dt, arr)
	}
}
