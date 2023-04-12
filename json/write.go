package json

import (
	"encoding/json"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

func (c *Client) WriteTableBatch(w io.Writer, _ *arrow.Schema, records []arrow.Record) error {
	for _, r := range records {
		err := c.writeRecord(w, r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (*Client) writeRecord(w io.Writer, record arrow.Record) error {
	arr := array.RecordToStructArray(record)
	defer arr.Release()
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for i := 0; i < arr.Len(); i++ {
		if err := enc.Encode(arr.GetOneForMarshal(i)); err != nil {
			return err
		}
	}
	return nil
}
