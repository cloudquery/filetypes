package csv

import (
	"encoding/csv"
	"io"

	"github.com/cloudquery/plugin-sdk/schema"
)

func (cl *Client) WriteTableBatch(w io.Writer, table *schema.Table, resources [][]any) error {
	writer := csv.NewWriter(w)
	writer.Comma = cl.Delimiter
	if cl.IncludeHeaders {
		cl.WriteTableHeaders(w, table)
	}
	for _, resource := range resources {
		record := make([]string, len(resource))
		for i, v := range resource {
			record[i] = v.(string)
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return nil
}

func (cl *Client) WriteTableHeaders(w io.Writer, table *schema.Table) error {
	writer := csv.NewWriter(w)
	writer.Comma = cl.Delimiter

	tableHeaders := make([]string, len(table.Columns))
	for index, header := range table.Columns {
		tableHeaders[index] = header.Name
	}
	if err := writer.Write(tableHeaders); err != nil {
		return err
	}
	writer.Flush()
	return nil
}
