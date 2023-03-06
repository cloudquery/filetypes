package json

import (
	"encoding/json"
	"io"

	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/cloudquery/filetypes/internal/cqarrow"
	"github.com/cloudquery/plugin-sdk/schema"
)

func (*Client) WriteTableBatch(w io.Writer, table *schema.Table, resources [][]any) error {
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

	arr := array.RecordToStructArray(record)
	defer arr.Release()
	enc := json.NewEncoder(w)
	for i := 0; i < arr.Len(); i++ {
		if err := enc.Encode(arr.GetOneForMarshal(i)); err != nil {
			return err
		}
	}
	return nil
}
