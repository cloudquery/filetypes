package json

import (
	"bufio"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

const maxJSONSize = 1024 * 1024 * 20

func (c *Client) Read(r io.Reader, arrowSchema *arrow.Schema, _ string, res chan<- arrow.Record) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, maxJSONSize), maxJSONSize)
	rb := array.NewRecordBuilder(c.mem, arrowSchema)
	defer rb.Release()
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
