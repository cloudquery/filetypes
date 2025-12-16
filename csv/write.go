package csv

import (
	// "encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

type Handle struct {
	w      *csv.Writer
	schema *arrow.Schema
}

var _ types.Handle = (*Handle)(nil)

func (cl *Client) WriteHeader(w io.Writer, t *schema.Table) (types.Handle, error) {
	s := t.ToArrowSchema()
	newSchema := convertSchema(s)
	writer := csv.NewWriter(w, newSchema,
		csv.WithComma(cl.Delimiter),
		csv.WithHeader(cl.IncludeHeaders),
		csv.WithNullWriter(""),
	)

	return &Handle{
		w:      writer,
		schema: newSchema,
	}, nil
}

func (h *Handle) WriteContent(records []arrow.RecordBatch) error {
	for _, record := range records {
		castRec := h.castToString(record)
		if err := h.w.Write(castRec); err != nil {
			return fmt.Errorf("failed to write record to csv: %w", err)
		}
	}

	if err := h.w.Flush(); err != nil {
		return fmt.Errorf("failed to flush csv writer: %w", err)
	}
	return nil
}

func (h *Handle) WriteFooter() error {
	return h.w.Flush()
}

func convertSchema(sch *arrow.Schema) *arrow.Schema {
	oldFields := sch.Fields()
	fields := make([]arrow.Field, len(oldFields))
	copy(fields, oldFields)
	for i, f := range fields {
		if !isTypeSupported(f.Type) {
			fields[i].Type = arrow.BinaryTypes.String
		}
		fields[i].Metadata = stripCQExtensionMetadata(fields[i].Metadata)
	}

	md := sch.Metadata()
	newSchema := arrow.NewSchema(fields, &md)
	return newSchema
}

func isTypeSupported(t arrow.DataType) bool {
	// list from arrow/csv/common.go
	switch t.(type) {
	case *arrow.BooleanType:
	case *arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type:
	case *arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type:
	case *arrow.Float32Type, *arrow.Float64Type:
	case *arrow.StringType:
	case *arrow.TimestampType:
	case *arrow.Date32Type, *arrow.Date64Type:
	case *arrow.Decimal128Type, *arrow.Decimal256Type:
	case *arrow.ListType:
	case *arrow.BinaryType:
	case arrow.ExtensionType:
		return true
	}

	return false
}

// castToString casts extension columns or unsupported columns to string. It does not release the original record.
func (h *Handle) castToString(rec arrow.RecordBatch) arrow.RecordBatch {
	cols := make([]arrow.Array, h.schema.NumFields())
	for c := 0; c < h.schema.NumFields(); c++ {
		col := rec.Column(c)
		if isTypeSupported(col.DataType()) {
			cols[c] = col
			continue
		}

		sb := array.NewStringBuilder(memory.DefaultAllocator)
		for i := 0; i < col.Len(); i++ {
			if col.IsNull(i) {
				sb.AppendNull()
				continue
			}
			sb.Append(col.ValueStr(i))
		}
		cols[c] = sb.NewArray()
	}
	return array.NewRecordBatch(h.schema, cols, rec.NumRows())
}

func stripCQExtensionMetadata(md arrow.Metadata) arrow.Metadata {
	m := md.ToMap()
	for k := range m {
		if strings.HasPrefix(k, "cq:extension:") {
			delete(m, k)
		}
	}
	return arrow.MetadataFrom(m)
}
