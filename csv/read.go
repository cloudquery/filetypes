package csv

import (
	"fmt"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/csv"
	"github.com/cloudquery/plugin-sdk/schema"
)

func (cl *Client) Read(r io.Reader, table *schema.Table, _ string, res chan<- arrow.Record) error {
	arrowSchema := table.ToArrowSchema()

	reader := csv.NewReader(r, arrowSchema, csv.WithComma(cl.Delimiter), csv.WithHeader(cl.IncludeHeaders), csv.WithNullReader(true, ""))
	sourceNameIndex := table.Columns.Index(schema.CqSourceNameColumn.Name)
	if sourceNameIndex == -1 {
		return fmt.Errorf("could not find column %s in table %s", schema.CqSourceNameColumn.Name, table.Name)
	}
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
