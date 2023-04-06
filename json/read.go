package json

import (
	"bufio"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/cloudquery/plugin-sdk/schema"
)

const maxJSONSize = 1024 * 1024 * 20

func (*Client) Read(r io.Reader, table *schema.Table, sourceName string, res chan<- arrow.Record) error {
	sourceNameIndex := table.Columns.Index(schema.CqSourceNameColumn.Name)
	if sourceNameIndex == -1 {
		return fmt.Errorf("could not find column %s in table %s", schema.CqSourceNameColumn.Name, table.Name)
	}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, maxJSONSize), maxJSONSize)
	arrowSchema := table.ToArrowSchema()
	rb := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer rb.Release()
	for scanner.Scan() {
		b := scanner.Bytes()
		err := rb.UnmarshalJSON(b)
		if err != nil {
			return err
		}
		r := rb.NewRecord()
		if r.Column(sourceNameIndex).(*array.String).Value(0) != sourceName {
			continue
		}
		res <- r
	}

	return scanner.Err()
}
