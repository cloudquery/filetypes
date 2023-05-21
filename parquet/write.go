package parquet

import (
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"
	"github.com/cloudquery/plugin-sdk/v3/schema"
)

func (*Client) WriteTableBatch(w io.Writer, table *schema.Table, records []arrow.Record) error {
	props := parquet.NewWriterProperties(
		parquet.WithVersion(parquet.V2_4),
		parquet.WithMaxRowGroupLength(128*1024*1024), // 128M
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	arrprops := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)
	sc := table.ToArrowSchema()
	fw, err := pqarrow.NewFileWriter(sc, w, props, arrprops)
	if err != nil {
		return err
	}
	for _, rec := range records {
		err := fw.WriteBuffered(rec)
		if err != nil {
			return err
		}
	}
	return fw.Close()
}
