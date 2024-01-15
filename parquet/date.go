package parquet

import (
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
)

func reverseTransformDate64(arr *array.Date32) *array.Date64 {
	builder := array.NewDate64Builder(memory.DefaultAllocator)

	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			builder.AppendNull()
			continue
		}

		builder.Append(arrow.Date64FromTime(arr.Value(i).ToTime()))
	}

	return builder.NewDate64Array()
}

func reverseTransformFromDate32(dt arrow.DataType, arr *array.Date32) arrow.Array {
	switch dt.(type) {
	case *arrow.Date32Type:
		return arr
	case *arrow.Date64Type:
		return reverseTransformDate64(arr)
	default:
		panic("unsupported " + dt.String() + " type in reverseTransformFromDate32")
	}
}
