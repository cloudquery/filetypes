package filetypes

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

func (cl *Client) WriteTableBatchFile(w io.Writer, table *schema.Table, records []arrow.Record) error {
	return types.WriteAll(cl, w, table, records)
}

func (cl *Client) WriteHeader(w io.Writer, t *schema.Table) (types.Handle, error) {
	switch cl.spec.Compression {
	case CompressionTypeNone:
		return cl.FileType.WriteHeaderRaw(w, t, types.NoopAfterFooterFunc)

	case CompressionTypeGZip:
		gw := gzip.NewWriter(w)
		return cl.FileType.WriteHeaderRaw(gw, t, func(_ types.Handle) error {
			return gw.Close()
		})

	default:
		return nil, fmt.Errorf("unhandled compression type %s", cl.spec.Compression)
	}
}
