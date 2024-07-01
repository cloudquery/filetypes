package csv

import (
	"fmt"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/csv"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

func (cl *Client) Read(r types.ReaderAtSeeker, table *schema.Table, res chan<- arrow.Record) error {
	arrowSchema := table.ToArrowSchema()
	newSchema := convertSchema(arrowSchema)
	reader := csv.NewReader(r, newSchema,
		csv.WithComma(cl.Delimiter),
		csv.WithHeader(cl.IncludeHeaders),
		csv.WithNullReader(true, ""),
	)
	for reader.Next() {
		if reader.Err() != nil {
			return reader.Err()
		}
		rec := reader.Record()
		castRec, err := reverseTransformRecord(rec, arrowSchema)
		if err != nil {
			return fmt.Errorf("failed to cast extension types: %w", err)
		}
		res <- castRec
	}
	return nil
}

func reverseTransformRecord(rec arrow.Record, sc *arrow.Schema) (arrow.Record, error) {
	if sc.Equal(rec.Schema()) {
		return rec, nil
	}

	cols := make([]arrow.Array, rec.NumCols())
	var err error
	for i, col := range rec.Columns() {
		cols[i], err = reverseTransformArray(col, sc.Field(i).Type)
		if err != nil {
			return nil, err
		}
	}
	return array.NewRecord(sc, cols, rec.NumRows()), nil
}

func reverseTransformArray(arr arrow.Array, dt arrow.DataType) (arrow.Array, error) {
	if arrow.TypeEqual(arr.DataType(), dt) {
		return arr, nil
	}

	if str, ok := arr.(*array.String); ok {
		return reverseTransformArrayFromString(str, dt)
	}

	// only lists left
	listDT, listArr := dt.(arrow.ListLikeType), arr.(array.ListLike)
	elems, err := reverseTransformArray(listArr.ListValues(), listDT.Elem())
	if err != nil {
		return nil, err
	}
	return array.MakeFromData(array.NewData(
		listDT, listArr.Len(),
		listArr.Data().Buffers(),
		[]arrow.ArrayData{elems.Data()},
		listArr.NullN(),
		listArr.Data().Offset(),
	)), nil
}

func reverseTransformArrayFromString(arr *array.String, dt arrow.DataType) (arrow.Array, error) {
	builder := array.NewBuilder(memory.DefaultAllocator, dt)
	builder.Reserve(arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			builder.AppendNull()
			continue
		}
		if err := builder.AppendValueFromString(arr.Value(i)); err != nil {
			return nil, err
		}
	}
	return builder.NewArray(), nil
}
