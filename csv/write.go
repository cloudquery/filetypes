package csv

import (
	// "encoding/csv"
	"fmt"
	"io"

	"github.com/apache/arrow/go/arrow/memory"
	"github.com/apache/arrow/go/v12/arrow/csv"
	"github.com/cloudquery/filetypes/internal/cqarrow"
	"github.com/cloudquery/plugin-sdk/schema"
)

func (cl *Client) WriteTableBatch(w io.Writer, table *schema.Table, resources [][]any) error {
	arrowSchema := cqarrow.CQSchemaToArrow(table)
	cqTypes := make([]schema.CQTypes, len(resources))
	for i := range resources {
		cqTypes[i] = make(schema.CQTypes, len(resources[i]))
		for j := range resources[i] {
			cqTypes[i][j] = resources[i][j].(schema.CQType)
		}
	}
	record := cqarrow.CQTypesToRecord(memory.DefaultAllocator, cqTypes, arrowSchema)
	defer record.Release()

	writer := csv.NewWriter(w, arrowSchema, csv.WithComma(cl.Delimiter), csv.WithHeader(cl.IncludeHeaders))
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record to csv: %w", err)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush csv writer: %w", err)
	}
	return nil

	// writer := csv.NewWriter(w)
	// writer.Comma = cl.Delimiter
	// if cl.IncludeHeaders {
	// 	if err := cl.WriteTableHeaders(w, table); err != nil {
	// 		return err
	// 	}
	// }
	// for _, resource := range resources {
	// 	record := make([]string, len(resource))
	// 	for i, v := range resource {
	// 		record[i] = v.(string)
	// 	}
	// 	if err := writer.Write(record); err != nil {
	// 		return err
	// 	}
	// }
	// writer.Flush()
	// return nil
}

// func (cl *Client) WriteTableHeaders(w io.Writer, table *schema.Table) error {
// 	writer := csv.NewWriter(w)
// 	writer.Comma = cl.Delimiter

// 	tableHeaders := make([]string, len(table.Columns))
// 	for index, header := range table.Columns {
// 		tableHeaders[index] = header.Name
// 	}
// 	if err := writer.Write(tableHeaders); err != nil {
// 		return err
// 	}
// 	writer.Flush()
// 	return nil
// }
