package xlsx

import (
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/xuri/excelize/v2"
)

const (
	defaultSheetName = "data"
)

type Handle struct {
	w      io.Writer
	schema *arrow.Schema

	idx  int
	file *excelize.File
}

var _ types.Handle = (*Handle)(nil)

func (cl *Client) WriteHeader(w io.Writer, t *schema.Table) (types.Handle, error) {
	file := excelize.NewFile()

	err := file.SetSheetName("Sheet1", defaultSheetName)
	if err != nil {
		return nil, fmt.Errorf("failed to create new sheet: %w", err)
	}

	var cells []any
	for _, name := range t.Columns.Names() {
		cells = append(cells, name)
	}

	if err = file.SetSheetRow(defaultSheetName, "A1", &cells); err != nil {
		return nil, fmt.Errorf("failed to set header row: %w", err)
	}

	return &Handle{
		w:      w,
		schema: convertSchema(t.ToArrowSchema()),
		idx:    2,
		file:   file,
	}, nil
}

func (h *Handle) WriteContent(records []arrow.Record) error {
	for _, record := range records {
		record := h.castToString(record)
		for i := 0; i < int(record.NumRows()); i++ {
			cellname, err := excelize.CoordinatesToCellName(1, h.idx)
			if err != nil {
				return fmt.Errorf("failed to convert coordinates to cell name: %w", err)
			}
			var cells []any
			for j := 0; j < int(record.NumCols()); j++ {
				cells = append(cells, record.Column(j).GetOneForMarshal(i))
			}
			h.idx += 1
			if err := h.file.SetSheetRow(defaultSheetName, cellname, &cells); err != nil {
				return fmt.Errorf("failed to set row in stream writer: %w", err)
			}
		}
	}

	return nil
}

func (h *Handle) WriteFooter() error {
	return h.file.Write(h.w)
}

func convertSchema(sch *arrow.Schema) *arrow.Schema {
	oldFields := sch.Fields()
	fields := make([]arrow.Field, len(oldFields))
	copy(fields, oldFields)
	for i, f := range fields {
		if !isTypeSupported(f.Type) {
			fields[i].Type = arrow.BinaryTypes.String
		}
	}

	md := sch.Metadata()
	newSchema := arrow.NewSchema(fields, &md)
	return newSchema
}

func isTypeSupported(t arrow.DataType) bool {
	switch t.(type) {
	case *arrow.BooleanType,
		*arrow.Int8Type, *arrow.Int16Type, *arrow.Int32Type, *arrow.Int64Type,
		*arrow.Uint8Type, *arrow.Uint16Type, *arrow.Uint32Type, *arrow.Uint64Type,
		*arrow.Float32Type, *arrow.Float64Type,
		*arrow.StringType,
		*arrow.TimestampType,
		*arrow.Date32Type, *arrow.Date64Type,
		*arrow.Decimal128Type, *arrow.Decimal256Type,
		*arrow.BinaryType:
		return true
	}

	return false
}

func (h *Handle) castToString(rec arrow.Record) arrow.Record {
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
	return array.NewRecord(h.schema, cols, rec.NumRows())
}
