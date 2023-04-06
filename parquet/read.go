package parquet

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/cloudquery/plugin-sdk/schema"
)

type ReaderAtSeeker interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

func (*Client) Read(f ReaderAtSeeker, table *schema.Table, sourceName string, res chan<- arrow.Record) error {
	sourceNameIndex := int64(table.Columns.Index(schema.CqSourceNameColumn.Name))
	if sourceNameIndex == -1 {
		return fmt.Errorf("could not find column %s in table %s", schema.CqSourceNameColumn.Name, table.Name)
	}

	mem := memory.DefaultAllocator
	ctx := context.Background()
	props := &parquet.ReaderProperties{
		BufferSize:            0,
		FileDecryptProps:      nil,
		BufferedStreamEnabled: false,
	}
	rdr, err := file.NewParquetReader(f, file.WithReadProps(props))
	if err != nil {
		return err
	}
	arrProps := pqarrow.ArrowReadProperties{
		Parallel:  false,
		BatchSize: 0,
	}
	fr, err := pqarrow.NewFileReader(rdr, arrProps, mem)

	rr, err := fr.GetRecordReader(ctx, nil, nil)
	for rr.Next() {
		rec, err := rr.Read()
		if err != nil {
			return err
		}
		res <- rec
	}

	return nil
}
