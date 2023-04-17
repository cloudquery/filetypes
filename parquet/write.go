package parquet

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/cloudquery/plugin-sdk/v2/types"
)

func (*Client) WriteTableBatch(w io.Writer, arrowSchema *arrow.Schema, records []arrow.Record) error {
	props := parquet.NewWriterProperties()
	arrprops := pqarrow.DefaultWriterProps()
	newSchema := convertSchema(arrowSchema)
	fw, err := pqarrow.NewFileWriter(newSchema, w, props, arrprops)
	if err != nil {
		return err
	}
	mem := memory.DefaultAllocator
	for _, rec := range records {
		castRec, err := castExtensionColsToString(mem, rec)
		if err != nil {
			return fmt.Errorf("failed to cast to string: %w", err)
		}
		if err := fw.Write(castRec); err != nil {
			return err
		}
	}
	return fw.Close()
}

func convertSchema(sch *arrow.Schema) *arrow.Schema {
	oldFields := sch.Fields()
	fields := make([]arrow.Field, len(oldFields))
	copy(fields, oldFields)
	for i, f := range fields {
		switch {
		case f.Type.ID() == arrow.EXTENSION:
			fields[i].Type = arrow.BinaryTypes.String
		case arrow.TypeEqual(f.Type, arrow.ListOf(types.NewUUIDType())),
			arrow.TypeEqual(f.Type, arrow.ListOf(types.NewInetType())),
			arrow.TypeEqual(f.Type, arrow.ListOf(types.NewJSONType())),
			arrow.TypeEqual(f.Type, arrow.ListOf(types.NewMacType())):
			fields[i].Type = arrow.ListOf(arrow.BinaryTypes.String)
		}
	}

	md := sch.Metadata()
	newSchema := arrow.NewSchema(fields, &md)
	return newSchema
}

func castExtensionColsToString(mem memory.Allocator, rec arrow.Record) (arrow.Record, error) {
	newSchema := convertSchema(rec.Schema())
	rb := array.NewRecordBuilder(mem, newSchema)

	defer rb.Release()
	for c := 0; c < int(rec.NumCols()); c++ {
		col := rec.Column(c)
		switch {
		case arrow.TypeEqual(col.DataType(), types.NewUUIDType()):
			arr := col.(*types.UUIDArray)
			b, err := arr.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
			}
			err = rb.Field(c).UnmarshalJSON(b)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
			}
		case arrow.TypeEqual(col.DataType(), types.NewInetType()):
			arr := col.(*types.InetArray)
			b, err := arr.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
			}
			err = rb.Field(c).UnmarshalJSON(b)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
			}
		case arrow.TypeEqual(col.DataType(), types.NewJSONType()):
			arr := col.(*types.JSONArray)
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
					rb.Field(c).(*array.StringBuilder).AppendNull()
					continue
				}
				b, err := json.Marshal(v)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
				}
				rb.Field(c).(*array.StringBuilder).Append(string(b))
			}
		case arrow.TypeEqual(col.DataType(), types.NewMacType()):
			arr := col.(*types.MacArray)
			b, err := arr.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
			}
			err = rb.Field(c).UnmarshalJSON(b)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
			}
		case arrow.TypeEqual(col.DataType(), arrow.ListOf(types.NewUUIDType())),
			arrow.TypeEqual(col.DataType(), arrow.ListOf(types.NewInetType())),
			arrow.TypeEqual(col.DataType(), arrow.ListOf(types.NewMacType())):
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
