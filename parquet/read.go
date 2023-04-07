package parquet

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/compute"
	"github.com/apache/arrow/go/v12/arrow/memory"
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
	arrowSchema := table.ToArrowSchema()
	sourceNameIndex := int64(table.Columns.Index(schema.CqSourceNameColumn.Name))
	if sourceNameIndex == -1 {
		return fmt.Errorf("could not find column %s in table %s", schema.CqSourceNameColumn.Name, table.Name)
	}
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
		castRec, err := castRecord(mem, rec, arrowSchema)
		if err != nil {
			return err
		}
		res <- castRec
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

func castRecord(mem memory.Allocator, rec arrow.Record, arrowSchema *arrow.Schema) (arrow.Record, error) {
	ctx := context.Background()
	rb := array.NewRecordBuilder(mem, arrowSchema)
	defer rb.Release()
	for c := 0; c < int(rec.NumCols()); c++ {
		arr, err := compute.CastToType(ctx, rec.Column(c), arrowSchema.Field(c).Type)
		if err != nil {
			return nil, fmt.Errorf("failed to cast col %v to %v: %w", rec.ColumnName(c), arrowSchema.Field(c).Type, err)
		}
		b, err := arr.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
		}
		err = rb.Field(c).UnmarshalJSON(b)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
		}
	}
	return rb.NewRecord(), nil
}
