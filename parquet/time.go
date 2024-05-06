package parquet

import (
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

func reverseTransformTime32(dt *arrow.Time32Type, arr *array.Time32) arrow.Array {
	builder := array.NewTime32Builder(memory.DefaultAllocator, dt)

	rescale := func() func(t arrow.Time32) arrow.Time32 {
		switch arr.DataType().(*arrow.Time32Type).Unit {
		case arrow.Second:
			switch dt.Unit {
			case arrow.Second:
				return func(t arrow.Time32) arrow.Time32 { return t }
			case arrow.Millisecond:
				return func(t arrow.Time32) arrow.Time32 { return t * 1e3 }
			default:
				panic("unsupported time32 time unit: " + dt.Unit.String())
			}
		case arrow.Millisecond:
			switch dt.Unit {
			case arrow.Second:
				return func(t arrow.Time32) arrow.Time32 { return t / 1e3 }
			case arrow.Millisecond:
				return func(t arrow.Time32) arrow.Time32 { return t }
			default:
				panic("unsupported time32 time unit: " + dt.Unit.String())
			}
		default:
			panic("unsupported time32 time unit: " + arr.DataType().(*arrow.Time32Type).Unit.String())
		}
	}()

	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			builder.AppendNull()
			continue
		}

		builder.Append(rescale(arr.Value(i)))
	}

	return builder.NewArray()
}

func reverseTransformTime64(dt *arrow.Time64Type, arr *array.Time64) arrow.Array {
	builder := array.NewTime64Builder(memory.DefaultAllocator, dt)

	rescale := func() func(t arrow.Time64) arrow.Time64 {
		switch arr.DataType().(*arrow.Time64Type).Unit {
		case arrow.Microsecond:
			switch dt.Unit {
			case arrow.Microsecond:
				return func(t arrow.Time64) arrow.Time64 { return t }
			case arrow.Nanosecond:
				return func(t arrow.Time64) arrow.Time64 { return t * 1e3 }
			default:
				panic("unsupported time64 time unit: " + dt.Unit.String())
			}
		case arrow.Nanosecond:
			switch dt.Unit {
			case arrow.Microsecond:
				return func(t arrow.Time64) arrow.Time64 { return t / 1e3 }
			case arrow.Nanosecond:
				return func(t arrow.Time64) arrow.Time64 { return t }
			default:
				panic("unsupported time64 time unit: " + dt.Unit.String())
			}
		default:
			panic("unsupported time64 time unit: " + arr.DataType().(*arrow.Time64Type).Unit.String())
		}
	}()

	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			builder.AppendNull()
			continue
		}

		builder.Append(rescale(arr.Value(i)))
	}

	return builder.NewArray()
}
