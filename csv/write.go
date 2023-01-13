package csv

import (
	"encoding/csv"
	"io"

	"github.com/cloudquery/plugin-sdk/schema"
)

func WriteTableBatch(w io.Writer, table *schema.Table, resources [][]any, headers bool) error {
	writer := csv.NewWriter(w)
	if headers {
		tableHeaders := make([]string, len(table.Columns))
		for index, header := range table.Columns {
			tableHeaders[index] = header.Name
		}
		if err := writer.Write(tableHeaders); err != nil {
			return err
		}
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
