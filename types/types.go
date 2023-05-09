package types

import (
	"io"

	"github.com/apache/arrow/go/v13/arrow"
)

type FileType interface {
	WriteHeader(io.Writer, *arrow.Schema) (Handle, error)
}

type Handle interface {
	WriteContent([]arrow.Record) error
	WriteFooter() error
}

func WriteAll(f FileType, w io.Writer, arrowSchema *arrow.Schema, records []arrow.Record) error {
	h, err := f.WriteHeader(w, arrowSchema)
	if err != nil {
		return err
	}
	if err := h.WriteContent(records); err != nil {
		return err
	}

	return h.WriteFooter()
}
