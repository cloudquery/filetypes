package parquet

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/cloudquery/plugin-sdk/v2/types"
)

type ReaderAtSeeker interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

func (c *Client) Read(f ReaderAtSeeker, arrowSchema *arrow.Schema, _ string, res chan<- arrow.Record) error {
	ctx := context.Background()
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		return fmt.Errorf("failed to create new parquet reader: %w", err)
	}
	arrProps := pqarrow.ArrowReadProperties{
		Parallel:  false,
		BatchSize: 1024,
	}
	fr, err := pqarrow.NewFileReader(rdr, arrProps, c.mem)
	if err != nil {
		return fmt.Errorf("failed to create new parquet file reader: %w", err)
	}
	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to get parquet record reader: %w", err)
	}
	defer rr.Release()
	for rr.Next() {
		rec := rr.Record()
		castRec, err := castStringsToExtensions(c.mem, rec, arrowSchema)
		if err != nil {
			return fmt.Errorf("failed to cast extension types: %w", err)
		}
		res <- castRec
	}
	if rr.Err() != nil && rr.Err() != io.EOF {
		return fmt.Errorf("failed to read parquet record: %w", rr.Err())
	}

	return nil
}

// castExtensionColsToString casts extension columns to string. It does not release the original record.
func castStringsToExtensions(mem memory.Allocator, rec arrow.Record, arrowSchema *arrow.Schema) (arrow.Record, error) {
	rb := array.NewRecordBuilder(mem, arrowSchema)

	defer rb.Release()
	for c := 0; c < int(rec.NumCols()); c++ {
		col := rec.Column(c)
		switch {
		case arrow.TypeEqual(arrowSchema.Field(c).Type, types.NewUUIDType()):
			arr := col.(*array.String)
			b, err := arr.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
			}
			err = rb.Field(c).UnmarshalJSON(b)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
			}
		case arrow.TypeEqual(arrowSchema.Field(c).Type, types.NewInetType()):
			arr := col.(*array.String)
			b, err := arr.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
			}
			err = rb.Field(c).UnmarshalJSON(b)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
			}
		case arrow.TypeEqual(arrowSchema.Field(c).Type, types.NewJSONType()):
			arr := col.(*array.String)
			b, err := arr.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
			}
			a := make([]any, arr.Len())
			err = json.Unmarshal(b, &a)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
			}
			for _, v := range a {
				if v == nil {
					rb.Field(c).(*types.JSONBuilder).AppendNull()
					continue
				}
				var v2 any
				err = json.Unmarshal([]byte(v.(string)), &v2)
				if err != nil {
					return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
				}
				rb.Field(c).(*types.JSONBuilder).Append(v2)
			}
		case arrow.TypeEqual(arrowSchema.Field(c).Type, types.NewMacType()):
			arr := col.(*array.String)
			b, err := arr.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
			}
			err = rb.Field(c).UnmarshalJSON(b)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
			}
		case arrow.TypeEqual(arrowSchema.Field(c).Type, arrow.ListOf(types.NewUUIDType())),
			arrow.TypeEqual(arrowSchema.Field(c).Type, arrow.ListOf(types.NewInetType())),
			arrow.TypeEqual(arrowSchema.Field(c).Type, arrow.ListOf(types.NewMacType())):
			arr := col.(*array.List)
			b, err := arr.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
			}
			err = rb.Field(c).UnmarshalJSON(b)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
			}
		default:
			b, err := rec.Column(c).MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
			}
			err = rb.Field(c).UnmarshalJSON(b)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
			}
		}
	}
	return rb.NewRecord(), nil
}
