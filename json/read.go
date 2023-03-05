package json

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"

	"github.com/cloudquery/plugin-sdk/schema"
)

const maxJSONSize = 1024 * 1024 * 20

func (*Client) Read(f io.Reader, table *schema.Table, sourceName string, res chan<- []any) error {
	sourceNameIndex := table.Columns.Index(schema.CqSourceNameColumn.Name)
	if sourceNameIndex == -1 {
		return fmt.Errorf("could not find column %s in table %s", schema.CqSourceNameColumn.Name, table.Name)
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, maxJSONSize), maxJSONSize)
	// arrowSchema := cqarrow.CQSchemaToArrow(table)
	// st := arrow.StructOf(arrowSchema.Fields()...)
	// arr, _, err := array.FromJSON(memory.DefaultAllocator, st, f)
	// if err != nil {
	// 	return err
	// }
	// bldr := array.NewStructBuilder(memory.DefaultAllocator, st)
	// defer bldr.Release()

	// dec := json.NewDecoder(r)
	// defer func() {
	// 	if errors.Is(err, io.EOF) {
	// 		err = fmt.Errorf("failed parsing json: %w", io.ErrUnexpectedEOF)
	// 	}
	// }()
	// res <- arr
	// arr.Data()
	for scanner.Scan() {
		jsonObj := make(map[string]any, len(table.Columns))
		row := scanner.Bytes()
		if err := json.Unmarshal(row, &jsonObj); err != nil {
			return err
		}
		if jsonObj[schema.CqSourceNameColumn.Name] != sourceName {
			continue
		}
		jsonArr := make([]any, len(table.Columns))
		for i, col := range table.Columns {
			jsonArr[i] = jsonObj[col.Name]
		}
		res <- jsonArr
	}

	return scanner.Err()
}
