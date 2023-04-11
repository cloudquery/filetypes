package parquet

import (
	"context"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
)

type ReaderAtSeeker interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

func (*Client) Read(f ReaderAtSeeker, arrowSchema *arrow.Schema, sourceName string, res chan<- arrow.Record) error {
	mem := memory.DefaultAllocator
	ctx := context.Background()
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		return err
	}
	arrProps := pqarrow.ArrowReadProperties{
		Parallel:  false,
		BatchSize: 1024,
	}
	fr, err := pqarrow.NewFileReader(rdr, arrProps, mem)
	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return err
	}
	for rr.Next() {
		rec := rr.Record()
		rec.Retain()
		res <- rec
		_, err = rr.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}
	rr.Release()

	return nil
}
