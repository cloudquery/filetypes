package csv

import (
	// "encoding/csv"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/csv"
	"github.com/cloudquery/plugin-sdk/schema"
)

func (cl *Client) WriteTableBatch(w io.Writer, table *schema.Table, records []arrow.Record) error {
	arrowSchema := table.ToArrowSchema()
	writer := csv.NewWriter(w, arrowSchema, csv.WithComma(cl.Delimiter), csv.WithHeader(cl.IncludeHeaders), csv.WithNullWriter(""))
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record to csv: %w", err)
		}
		if err := writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush csv writer: %w", err)
		}
	}
	return nil
}
