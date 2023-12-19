package json

import (
	"io"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/goccy/go-json"
)

type Handle struct {
	w io.Writer
}

var _ types.Handle = (*Handle)(nil)

func (*Client) WriteHeader(w io.Writer, _ *schema.Table) (types.Handle, error) {
	return &Handle{
		w: w,
	}, nil
}

func (*Handle) WriteFooter() error {
	return nil
}

func (h *Handle) WriteContent(records []arrow.Record) error {
	for _, r := range records {
		err := writeRecord(h.w, r)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeRecord(w io.Writer, record arrow.Record) error {
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
