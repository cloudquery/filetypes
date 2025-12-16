package types

import (
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

type ReaderAtSeeker interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

type FileType interface {
	WriteHeader(io.Writer, *schema.Table) (Handle, error)
	Read(ReaderAtSeeker, *schema.Table, chan<- arrow.RecordBatch) error
}

type Handle interface {
	WriteContent([]arrow.RecordBatch) error
	WriteFooter() error
}

func WriteAll(f FileType, w io.Writer, t *schema.Table, records []arrow.RecordBatch) error {
	h, err := f.WriteHeader(w, t)
	if err != nil {
		return err
	}
	if err := h.WriteContent(records); err != nil {
		return err
	}

	return h.WriteFooter()
}
