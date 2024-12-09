package filetypes

import (
	"bytes"
	"compress/gzip"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

func (cl *Client) Read(f types.ReaderAtSeeker, table *schema.Table, res chan<- arrow.Record) error {
	if cl.spec.Compression == CompressionTypeGZip {
		rr, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer rr.Close()
		b, err := io.ReadAll(rr)
		if err != nil {
			return err
		}
		f = bytes.NewReader(b)
	}

	return cl.filetype.Read(f, table, res)
}
