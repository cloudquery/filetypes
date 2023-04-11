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
		//castRec, err := castRecord(mem, rec, arrowSchema)
		//if err != nil {
		//	return err
		//}
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

//
//func castRecord(mem memory.Allocator, rec arrow.Record, arrowSchema *arrow.Schema) (arrow.Record, error) {
//	ctx := context.Background()
//	rb := array.NewRecordBuilder(mem, arrowSchema)
//	defer rb.Release()
//	for c := 0; c < int(rec.NumCols()); c++ {
//		tp := arrowSchema.Field(c).Type
//		if arrowSchema.Field(c).Type.ID() == arrow.EXTENSION {
//			tp = arrowSchema.Field(c).Type.(arrow.ExtensionType).StorageType()
//		}
//		arr, err := compute.CastToType(ctx, rec.Column(c), tp)
//		if err != nil {
//			return nil, fmt.Errorf("failed to cast col %v to %v: %w", rec.ColumnName(c), arrowSchema.Field(c).Type, err)
//		}
//		// Ideally these cases should be handled by Arrow itself.
//		if arrowSchema.Field(c).Type.ID() == arrow.EXTENSION {
//			switch {
//			case arrowSchema.Field(c).Type.(arrow.ExtensionType).ExtensionEquals(types.NewUUIDType()):
//				bldr := array.NewExtensionBuilder(mem, types.NewUUIDType())
//				uuidBuilder := types.NewUUIDBuilder(bldr)
//				j, err := arr.MarshalJSON()
//				if err != nil {
//					return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
//				}
//				uuidBuilder.StorageBuilder().UnmarshalJSON(j)
//				arr.Release()
//				arr = uuidBuilder.NewArray()
//				uuidBuilder.Release()
//			case arrowSchema.Field(c).Type.(arrow.ExtensionType).ExtensionEquals(types.NewJSONType()):
//				bldr := array.NewExtensionBuilder(mem, types.NewJSONType())
//				jsonBuilder := types.NewJSONBuilder(bldr)
//				j, err := arr.MarshalJSON()
//				if err != nil {
//					return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
//				}
//				jsonBuilder.StorageBuilder().UnmarshalJSON(j)
//				arr.Release()
//				arr = jsonBuilder.NewArray()
//			case arrowSchema.Field(c).Type.(arrow.ExtensionType).ExtensionEquals(types.NewInetType()):
//				bldr := array.NewExtensionBuilder(mem, types.NewInetType())
//				inetBuilder := types.NewInetBuilder(bldr)
//				j, err := arr.MarshalJSON()
//				if err != nil {
//					return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
//				}
//				inetBuilder.StorageBuilder().UnmarshalJSON(j)
//				arr.Release()
//				arr = inetBuilder.NewArray()
//			}
//		} else if arrow.TypeEqual(arrowSchema.Field(c).Type, arrow.ListOf(types.NewUUIDType())) {
//			// TODO: handle
//			continue
//		} else {
//			b, err := arr.MarshalJSON()
//			if err != nil {
//				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
//			}
//			err = rb.Field(c).UnmarshalJSON(b)
//			if err != nil {
//				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
//			}
//		}
//	}
//	return rb.NewRecord(), nil
//}
