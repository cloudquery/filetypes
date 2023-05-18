package parquet

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"
	"github.com/cloudquery/plugin-sdk/v3/schema"
	"github.com/cloudquery/plugin-sdk/v3/types"
)

func (c *Client) WriteTableBatch(w io.Writer, table *schema.Table, records []arrow.Record) error {
	props := parquet.NewWriterProperties(
		parquet.WithMaxRowGroupLength(128*1024*1024), // 128M
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	arrprops := pqarrow.DefaultWriterProps()
	newSchema := convertSchema(table.ToArrowSchema())
	fw, err := pqarrow.NewFileWriter(newSchema, w, props, arrprops)
	if err != nil {
		return err
	}
	for _, rec := range records {
		err := c.writeRecord(rec, fw)
		if err != nil {
			return err
		}
	}
	return fw.Close()
}

func (*Client) writeRecord(rec arrow.Record, fw *pqarrow.FileWriter) error {
	castRec, err := castExtensionColsToString(rec)
	if err != nil {
		return fmt.Errorf("failed to cast to string: %w", err)
	}
	return fw.Write(castRec)
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
			arrow.TypeEqual(f.Type, arrow.ListOf(types.NewMACType())):
			fields[i].Type = arrow.ListOf(arrow.BinaryTypes.String)
		}
	}

	md := sch.Metadata()
	newSchema := arrow.NewSchema(fields, &md)
	return newSchema
}

// castExtensionColsToString casts extension columns to string. It does not release the original record.
func castExtensionColsToString(rec arrow.Record) (arrow.Record, error) {
	newSchema := convertSchema(rec.Schema())
	cols := make([]arrow.Array, rec.NumCols())
	for c := 0; c < int(rec.NumCols()); c++ {
		col := rec.Column(c)
		switch {
		case arrow.TypeEqual(col.DataType(), types.NewUUIDType()):
			arr := col.(*types.UUIDArray)
			b, err := arr.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
			}
			sb := array.NewStringBuilder(memory.DefaultAllocator)
			err = sb.UnmarshalJSON(b)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
			}
			cols[c] = sb.NewArray()
		case arrow.TypeEqual(col.DataType(), types.NewInetType()):
			arr := col.(*types.InetArray)
			b, err := arr.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
			}
			sb := array.NewStringBuilder(memory.DefaultAllocator)
			err = sb.UnmarshalJSON(b)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
			}
			cols[c] = sb.NewArray()
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
			sb := array.NewStringBuilder(memory.DefaultAllocator)
			for _, v := range a {
				if v == nil {
					sb.AppendNull()
					continue
				}
				b, err := json.Marshal(v)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
				}
				sb.Append(string(b))
			}
			cols[c] = sb.NewArray()
		case arrow.TypeEqual(col.DataType(), types.NewMACType()):
			arr := col.(*types.MACArray)
			b, err := arr.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
			}
			sb := array.NewStringBuilder(memory.DefaultAllocator)
			err = sb.UnmarshalJSON(b)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
			}
			cols[c] = sb.NewArray()
		case arrow.TypeEqual(col.DataType(), arrow.ListOf(types.NewUUIDType())),
			arrow.TypeEqual(col.DataType(), arrow.ListOf(types.NewInetType())),
			arrow.TypeEqual(col.DataType(), arrow.ListOf(types.NewMACType())):
			arr := col.(*array.List)
			b, err := arr.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
			}
			sb := array.NewListBuilder(memory.DefaultAllocator, arrow.BinaryTypes.String)
			err = sb.UnmarshalJSON(b)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
			}
			cols[c] = sb.NewArray()
		default:
			cols[c] = rec.Column(c)
		}
	}
	return array.NewRecord(newSchema, cols, rec.NumRows()), nil
}
