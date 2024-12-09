package filetypes

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

func (cl *Client) WriteTableBatchFile(w io.Writer, table *schema.Table, records []arrow.Record) error {
	return types.WriteAll(cl, w, table, records)
}

func (cl *Client) WriteHeader(w io.Writer, t *schema.Table) (h types.Handle, retErr error) {
	defer func() {
		if msg := recover(); msg != nil {
			switch v := msg.(type) {
			case error:
				retErr = fmt.Errorf("panic: %w [recovered]", v)
			default:
				retErr = fmt.Errorf("panic: %v [recovered]", msg)
			}
		}
	}()

	switch cl.spec.Compression {
	case CompressionTypeNone:
		return cl.filetype.WriteHeader(w, t)

	case CompressionTypeGZip:
		gw := gzip.NewWriter(w)
		h, err := cl.filetype.WriteHeader(gw, t)
		if err != nil {
			return nil, err
		}
		return newClosableHandle(h, gw.Close), nil

	default:
		return nil, fmt.Errorf("unhandled compression type %s", cl.spec.Compression)
	}
}

type closableHandle struct {
	types.Handle
	afterCloseFunc func() error
}

var _ types.Handle = (*closableHandle)(nil)

func newClosableHandle(h types.Handle, afterCloseFunc func() error) types.Handle {
	return &closableHandle{Handle: h, afterCloseFunc: afterCloseFunc}
}
func (c *closableHandle) WriteFooter() error {
	if err := c.Handle.WriteFooter(); err != nil {
		return err
	}
	return c.afterCloseFunc()
}
