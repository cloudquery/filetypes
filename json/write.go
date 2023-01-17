package json

import (
	"encoding/json"

	"github.com/cloudquery/plugin-sdk/schema"
)

func (cl *Client) WriteTableBatch(table *schema.Table, resources [][]any) error {
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
		if _, err := cl.Writer.Write(b); err != nil {
			return err
		}
	}
	return nil
}
