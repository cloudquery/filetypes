package parquet

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestReverseTransformArray_Uint32ToUint64(t *testing.T) {
	builder := array.NewUint32Builder(memory.DefaultAllocator)
	defer builder.Release()

	builder.Append(0)
	builder.Append(42)
	builder.AppendNull()
	builder.Append(4294967295) // max uint32

	arr := builder.NewArray()
	defer arr.Release()

	result := reverseTransformArray(arrow.PrimitiveTypes.Uint64, arr)
	defer result.Release()

	require.Equal(t, arrow.PrimitiveTypes.Uint64, result.DataType())
	require.Equal(t, 4, result.Len())

	u64 := result.(*array.Uint64)
	require.Equal(t, uint64(0), u64.Value(0))
	require.False(t, u64.IsNull(0))

	require.Equal(t, uint64(42), u64.Value(1))
	require.False(t, u64.IsNull(1))

	require.True(t, u64.IsNull(2))

	require.Equal(t, uint64(4294967295), u64.Value(3))
	require.False(t, u64.IsNull(3))
}

func TestReverseTransformArray_Uint32ToUint64_Empty(t *testing.T) {
	builder := array.NewUint32Builder(memory.DefaultAllocator)
	defer builder.Release()

	arr := builder.NewArray()
	defer arr.Release()

	result := reverseTransformArray(arrow.PrimitiveTypes.Uint64, arr)
	defer result.Release()

	require.Equal(t, arrow.PrimitiveTypes.Uint64, result.DataType())
	require.Equal(t, 0, result.Len())
}

func TestReverseTransformArray_Uint32ToUint64_ListOf(t *testing.T) {
	bldr := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Uint32)
	defer bldr.Release()

	vb := bldr.ValueBuilder().(*array.Uint32Builder)

	bldr.Append(true)
	vb.Append(1)
	vb.Append(2)

	bldr.Append(true)
	vb.Append(3)

	bldr.AppendNull()

	arr := bldr.NewArray()
	defer arr.Release()

	targetDt := arrow.ListOf(arrow.PrimitiveTypes.Uint64)
	result := reverseTransformArray(targetDt, arr)
	defer result.Release()

	require.True(t, arrow.TypeEqual(targetDt, result.DataType()))
	require.Equal(t, 3, result.Len())

	listArr := result.(*array.List)
	require.False(t, listArr.IsNull(0))
	require.False(t, listArr.IsNull(1))
	require.True(t, listArr.IsNull(2))

	values := listArr.ListValues().(*array.Uint64)
	require.Equal(t, uint64(1), values.Value(0))
	require.Equal(t, uint64(2), values.Value(1))
	require.Equal(t, uint64(3), values.Value(2))
}

func TestReverseTransformArray_Int32ToUint64(t *testing.T) {
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	defer builder.Release()

	builder.Append(0)
	builder.Append(42)
	builder.AppendNull()
	builder.Append(2147483647) // max int32

	arr := builder.NewArray()
	defer arr.Release()

	result := reverseTransformArray(arrow.PrimitiveTypes.Uint64, arr)
	defer result.Release()

	require.Equal(t, arrow.PrimitiveTypes.Uint64, result.DataType())
	require.Equal(t, 4, result.Len())

	u64 := result.(*array.Uint64)
	require.Equal(t, uint64(0), u64.Value(0))
	require.False(t, u64.IsNull(0))

	require.Equal(t, uint64(42), u64.Value(1))
	require.False(t, u64.IsNull(1))

	require.True(t, u64.IsNull(2))

	require.Equal(t, uint64(2147483647), u64.Value(3))
	require.False(t, u64.IsNull(3))
}

func TestReverseTransformArray_Int32ToUint64_Empty(t *testing.T) {
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	defer builder.Release()

	arr := builder.NewArray()
	defer arr.Release()

	result := reverseTransformArray(arrow.PrimitiveTypes.Uint64, arr)
	defer result.Release()

	require.Equal(t, arrow.PrimitiveTypes.Uint64, result.DataType())
	require.Equal(t, 0, result.Len())
}

func TestReverseTransformArray_Int32ToUint64_ListOf(t *testing.T) {
	bldr := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int32)
	defer bldr.Release()

	vb := bldr.ValueBuilder().(*array.Int32Builder)

	bldr.Append(true)
	vb.Append(1)
	vb.Append(2)

	bldr.Append(true)
	vb.Append(3)

	bldr.AppendNull()

	arr := bldr.NewArray()
	defer arr.Release()

	targetDt := arrow.ListOf(arrow.PrimitiveTypes.Uint64)
	result := reverseTransformArray(targetDt, arr)
	defer result.Release()

	require.True(t, arrow.TypeEqual(targetDt, result.DataType()))
	require.Equal(t, 3, result.Len())

	listArr := result.(*array.List)
	require.False(t, listArr.IsNull(0))
	require.False(t, listArr.IsNull(1))
	require.True(t, listArr.IsNull(2))

	values := listArr.ListValues().(*array.Uint64)
	require.Equal(t, uint64(1), values.Value(0))
	require.Equal(t, uint64(2), values.Value(1))
	require.Equal(t, uint64(3), values.Value(2))
}

func TestReverseTransformArray_Int32ToUint64_NegativePanics(t *testing.T) {
	builder := array.NewInt32Builder(memory.DefaultAllocator)
	defer builder.Release()

	builder.Append(-1)

	arr := builder.NewArray()
	defer arr.Release()

	require.PanicsWithError(t, "negative int32 value -1 at index 0 cannot be converted to uint64", func() {
		reverseTransformArray(arrow.PrimitiveTypes.Uint64, arr)
	})
}
