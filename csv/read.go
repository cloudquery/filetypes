package csv

import (
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/csv"
	"github.com/cloudquery/plugin-sdk/v3/schema"
)

func (cl *Client) Read(r io.Reader, table *schema.Table, _ string, res chan<- arrow.Record) error {
	reader := csv.NewReader(r, table.ToArrowSchema(),
		csv.WithComma(cl.Delimiter),
		csv.WithHeader(cl.IncludeHeaders),
		csv.WithNullReader(true, ""),
	)
	for reader.Next() {
		if reader.Err() != nil {
			return reader.Err()
		}
		rec := reader.Record()
		rec.Retain()
		res <- rec
	}
	return nil
}
