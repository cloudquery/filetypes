package xlsx

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/goccy/go-json"
	"github.com/xuri/excelize/v2"
)

func (cl *Client) Read(r types.ReaderAtSeeker, table *schema.Table, res chan<- arrow.Record) error {
	file, err := excelize.OpenReader(r)
	if err != nil {
		return fmt.Errorf("failed to open xlsx reader: %w", err)
	}

	sheetName := "data"
	rows, err := file.GetRows(sheetName)
	if err != nil {
		return fmt.Errorf("failed to get rows from sheet %s: %w", sheetName, err)
	}

	for _, row := range rows {
		rb := array.NewRecordBuilder(memory.DefaultAllocator, table.ToArrowSchema())
		for i, field := range rb.Fields() {
			err := appendValue(field, row[i])
			if err != nil {
				return fmt.Errorf("failed to read from sheet %s: %w", table.Name, err)
			}
		}
		res <- rb.NewRecord()
	}
	return nil
}

func appendValue(builder array.Builder, value any) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}
	switch bldr := builder.(type) {
	case array.ListLikeBuilder:
		lst := value.([]any)
		if lst == nil {
			bldr.AppendNull()
			return nil
		}
		bldr.Append(true)
		valBuilder := bldr.ValueBuilder()
		for _, v := range lst {
			if err := appendValue(valBuilder, v); err != nil {
				return err
			}
		}
		return nil
	case *array.StructBuilder:
		m := value.(map[string]any)
		bldr.Append(true)
		bldrType := bldr.Type().(*arrow.StructType)
		for k, v := range m {
			idx, _ := bldrType.FieldIdx(k)
			fieldBldr := bldr.FieldBuilder(idx)
			if err := appendValue(fieldBldr, v); err != nil {
				return err
			}
		}
		return nil
	case *array.MonthIntervalBuilder, *array.DayTimeIntervalBuilder, *array.MonthDayNanoIntervalBuilder:
		b, err := json.Marshal(value)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(bytes.NewReader(b))
		return bldr.UnmarshalOne(dec)
	case *array.Int8Builder, *array.Int16Builder, *array.Int32Builder, *array.Int64Builder:
		return bldr.AppendValueFromString(fmt.Sprintf("%d", int64(value.(float64))))
	case *array.Uint8Builder, *array.Uint16Builder, *array.Uint32Builder, *array.Uint64Builder:
		return bldr.AppendValueFromString(fmt.Sprintf("%d", uint64(value.(float64))))
	}
	return builder.AppendValueFromString(fmt.Sprintf("%v", value))
}
