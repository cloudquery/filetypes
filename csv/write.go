package csv

import (
	// "encoding/csv"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/csv"
)

func (cl *Client) WriteTableBatch(w io.Writer, arrowSchema *arrow.Schema, records []arrow.Record) error {
	writer := csv.NewWriter(w, arrowSchema,
		csv.WithComma(cl.Delimiter),
		csv.WithHeader(cl.IncludeHeaders),
		csv.WithNullWriter(""),
	)
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
