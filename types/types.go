package types

import (
	"io"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

type ReaderAtSeeker interface {
	io.Reader
	io.ReaderAt
	io.Seeker
}

type FileType interface {
	WriteHeader(io.Writer, *schema.Table) (Handle, error)
	Read(r ReaderAtSeeker, table *schema.Table, res chan<- arrow.Record) error
}

type Handle interface {
	WriteContent([]arrow.Record) error
	WriteFooter() error
}

func WriteAll(f FileType, w io.Writer, t *schema.Table, records []arrow.Record) error {
	h, err := f.WriteHeader(w, t)
	if err != nil {
		return err
	}
	if err := h.WriteContent(records); err != nil {
		return err
	}

	return h.WriteFooter()
}
