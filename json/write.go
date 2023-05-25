package json

import (
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/cloudquery/plugin-sdk/v3/schema"
	"github.com/goccy/go-json"
)

func (c *Client) WriteTableBatch(w io.Writer, _ *schema.Table, records []arrow.Record) error {
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
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for i := 0; i < arr.Len(); i++ {
		if err := enc.Encode(arr.GetOneForMarshal(i)); err != nil {
			return err
		}
	}
	return nil
}
