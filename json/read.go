package json

import (
	"bufio"
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

const maxJSONSize = 1024 * 1024 * 20

func (*Client) Read(r io.Reader, table *schema.Table, res chan<- arrow.Record) error {
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
