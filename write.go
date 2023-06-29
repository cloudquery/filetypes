package filetypes

import (
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

func (cl *Client) WriteTableBatchFile(w io.Writer, table *schema.Table, records []arrow.Record) error {
	return types.WriteAll(cl.FileType, w, table, records)
}
