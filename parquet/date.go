package parquet

import (
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
)

func reverseTransformDate32(arr *array.Timestamp, toTime toTimeFunc) arrow.Array {
	builder := array.NewDate32Builder(memory.DefaultAllocator)

	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			builder.AppendNull()
			continue
		}

		builder.Append(arrow.Date32FromTime(toTime(arr.Value(i))))
	}

	return builder.NewArray()
}

func reverseTransformDate64(arr *array.Timestamp, toTime toTimeFunc) arrow.Array {
	builder := array.NewDate64Builder(memory.DefaultAllocator)

	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			builder.AppendNull()
			continue
		}

		builder.Append(arrow.Date64FromTime(toTime(arr.Value(i))))
	}

	return builder.NewArray()
}
