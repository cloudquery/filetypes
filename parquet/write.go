package parquet

import (
	"io"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/parquet"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
)

func (*Client) WriteTableBatch(w io.Writer, arrowSchema *arrow.Schema, records []arrow.Record) error {
	props := parquet.NewWriterProperties()
	arrprops := pqarrow.DefaultWriterProps()
	fw, err := pqarrow.NewFileWriter(arrowSchema, w, props, arrprops)
	if err != nil {
		return err
	}
	for _, rec := range records {
		if err := fw.Write(rec); err != nil {
			return err
		}
	}
	return fw.Close()
	//parquetSchema, err := makeSchema(table.Name, table.Columns)
	//if err != nil {
	//	return fmt.Errorf("failed to create parquet schema: %w", err)
	//}
	//wf := writerfile.NewWriterFile(w)
	//arrowSchema := table.ToArrowSchema()
	//pw, err := writer.NewArrowWriter(arrowSchema, wf, 2)
	//if err != nil {
	//	return fmt.Errorf("can't create parquet writer: %w", err)
	//}
	//
	//pw, err := writer.NewJSONWriterFromWriter(parquetSchema, w, 2)
	//if err != nil {
	//	return fmt.Errorf("can't create parquet writer: %w", err)
	//}
	//
	//pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	//pw.CompressionType = parquet.CompressionCodec_SNAPPY
	//
	//for _, rec := range records {
	//	for r := int64(0); r < rec.NumRows(); r++ {
	//		m := rec.NewSlice(r, r+1)
	//		b, err := json.Marshal(m)
	//		if err != nil {
	//			return fmt.Errorf("can't marshal record: %w", err)
	//		}
	//		// hacky, but we don't have a better way to get json for a single row
	//		b = bytes.TrimPrefix(b, []byte("["))
	//		b = bytes.TrimSuffix(b, []byte("]"))
	//		if err := pw.Write(b); err != nil {
	//			return err
	//		}
	//	}
	//	pw.Flush(true)
	//}
	//
	//return pw.WriteStop()
}
