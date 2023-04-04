package json

import (
	"encoding/json"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/cloudquery/plugin-sdk/schema"
)

func (c *Client) WriteTableBatch(w io.Writer, _ *schema.Table, records []arrow.Record) error {
	for _, r := range records {
		err := c.writeTableBatch(w, r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) writeTableBatch(w io.Writer, record arrow.Record) error {
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
