package types

import (
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

type FileType interface {
	WriteHeader(io.Writer, *schema.Table) (Handle, error)
}

type Handle interface {
	WriteContent([]arrow.Record) error
	WriteFooter() error
}

type RawFileType interface {
	WriteHeaderRaw(io.Writer, *schema.Table, AfterFooterFunc) (Handle, error)
}

type AfterFooterFunc func(handle Handle) error

var NoopAfterFooterFunc = func(_ Handle) error { return nil }

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

func RawWriteAll(f RawFileType, w io.Writer, t *schema.Table, records []arrow.Record) error {
	h, err := f.WriteHeaderRaw(w, t, NoopAfterFooterFunc)
	if err != nil {
		return err
	}
	if err := h.WriteContent(records); err != nil {
		return err
	}

	return h.WriteFooter()
}
