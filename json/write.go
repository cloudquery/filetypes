package json

import (
	"encoding/json"
	"io"

	"github.com/cloudquery/plugin-sdk/schema"
)

func (cl *Client) WriteTableBatch(w io.Writer, table *schema.Table, resources [][]any) error {
	for _, resource := range resources {
		jsonObj := make(map[string]any, len(table.Columns))
		for i := range resource {
			jsonObj[table.Columns[i].Name] = resource[i]
		}
		b, err := json.Marshal(jsonObj)
		if err != nil {
			return err
		}
		b = append(b, '\n')
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	return nil
}
