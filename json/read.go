package json

import (
	"bufio"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

const maxJSONSize = 1024 * 1024 * 20

func (*Client) Read(r types.ReaderAtSeeker, table *schema.Table, res chan<- arrow.Record) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, maxJSONSize), maxJSONSize)
	rb := array.NewRecordBuilder(memory.DefaultAllocator, table.ToArrowSchema())
	for scanner.Scan() {
		b := scanner.Bytes()
		err := rb.UnmarshalJSON(b)
		if err != nil {
			return err
		}
		r := rb.NewRecord()
		res <- r
	}

	return scanner.Err()
}
