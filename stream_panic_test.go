package filetypes

import (
	"errors"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/stretchr/testify/require"
)

func TestPanicOnHeader(t *testing.T) {
	r := require.New(t)
	cl := &Client{
		spec: &FileSpec{
			Compression: CompressionTypeNone,
		},
		filetype: &customWriter{
			PanicOnHeader: true,
		},
	}

	stream, err := cl.StartStream(&schema.Table{}, func(io.Reader) error {
		return nil
	})
	r.Nil(stream)
	r.Error(err)
	r.ErrorContains(err, "panic:")
}

func TestPanicOnWrite(t *testing.T) {
	r := require.New(t)
	cl := &Client{
		spec: &FileSpec{
			Compression: CompressionTypeNone,
		},
		filetype: &customWriter{
			PanicOnWrite: true,
		},
	}

	table := &schema.Table{
		Name: "test",
		Columns: []schema.Column{
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
	}
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, table.ToArrowSchema())
	bldr.Field(0).(*array.StringBuilder).Append("foo")
	bldr.Field(0).(*array.StringBuilder).Append("bar")
	record := bldr.NewRecord()

	stream, err := cl.StartStream(table, func(io.Reader) error {
		return nil
	})
	r.NoError(err)
	err = stream.Write([]arrow.Record{record})
	r.Error(err)
	r.ErrorContains(err, "panic:")

	r.NoError(stream.Finish())
}

func TestPanicOnClose(t *testing.T) {
	r := require.New(t)
	cl := &Client{
		spec: &FileSpec{
			Compression: CompressionTypeNone,
		},
		filetype: &customWriter{
			PanicOnClose: true,
		},
	}

	stream, err := cl.StartStream(&schema.Table{}, func(io.Reader) error {
		return nil
	})
	r.NoError(err)
	r.NoError(stream.Write(nil))

	err = stream.Finish()
	r.Error(err)
	r.ErrorContains(err, "panic:")
}

type customWriter struct {
	PanicOnHeader bool
	PanicOnWrite  bool
	PanicOnClose  bool
}
type customHandle struct {
	w *customWriter
}

func (w *customWriter) WriteHeader(io.Writer, *schema.Table) (types.Handle, error) {
	if w.PanicOnHeader {
		panic("test panic")
	}
	return &customHandle{w: w}, nil
}

func (*customWriter) Read(types.ReaderAtSeeker, *schema.Table, chan<- arrow.Record) error {
	return errors.New("not implemented")
}

func (h *customHandle) WriteContent([]arrow.Record) error {
	if h.w.PanicOnWrite {
		panic("test panic")
	}
	return nil
}
func (h *customHandle) WriteFooter() error {
	if h.w.PanicOnClose {
		panic("test panic")
	}
	return nil
}
