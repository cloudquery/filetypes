package json

import (
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/goccy/go-json"
)

type Handle struct {
	w           io.Writer
	afterFooter types.AfterFooterFunc
}

var _ types.Handle = (*Handle)(nil)

func (*Client) WriteHeaderRaw(w io.Writer, _ *schema.Table, afterFooter types.AfterFooterFunc) (types.Handle, error) {
	return &Handle{
		w:           w,
		afterFooter: afterFooter,
	}, nil
}

func (h *Handle) WriteFooter() error {
	return h.afterFooter(h)
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
