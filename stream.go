package filetypes

import (
	"fmt"
	"io"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

// Stream helps with streaming uploads by handling header/footer and uploader logic. Use StartStream to start a stream and then Write to it.
type Stream struct {
	h    types.Handle
	wc   *writeCloser
	done chan error
}

type writeCloser struct {
	*io.PipeWriter
	closed bool
}

func (w *writeCloser) Close() error {
	w.closed = true
	return w.PipeWriter.Close()
}

// StartStream starts a streaming upload using the provided uploadFunc.
func (cl *Client) StartStream(table *schema.Table, uploadFunc func(io.Reader) error) (*Stream, error) {
	pr, pw := io.Pipe()
	doneCh := make(chan error)

	go func() {
		err := uploadFunc(pr)
		_ = pr.CloseWithError(err)
		doneCh <- err
		close(doneCh)
	}()

	wc := &writeCloser{PipeWriter: pw}
	h, err := cl.WriteHeader(wc, table)
	if err != nil {
		_ = pw.CloseWithError(err)
		<-doneCh
		return nil, err
	}

	return &Stream{
		h:    h,
		wc:   wc,
		done: doneCh,
	}, nil
}

// Write to the stream opened with StartStream.
func (s *Stream) Write(records []arrow.Record) error {
	if len(records) == 0 {
		return nil
	}

	return s.h.WriteContent(records)
}

// Finish writing to the stream.
func (s *Stream) Finish() error {
	return s.FinishWithError(nil)
}

// FinishWithError aborts writing to the stream by closing the writer with the provided error and waiting for the uploader to finish.
func (s *Stream) FinishWithError(finishError error) error {
	if finishError != nil {
		_ = s.wc.CloseWithError(finishError)
		return <-s.done
	}

	if err := s.h.WriteFooter(); err != nil {
		if !s.wc.closed {
			_ = s.wc.CloseWithError(err)
		}
		return fmt.Errorf("failed to write footer: %w", <-s.done)
	}

	// ParquetWriter likes to close the underlying writer, so we need to check if it's already closed
	if !s.wc.closed {
		if err := s.wc.Close(); err != nil {
			return err
		}
	}

	return <-s.done
}
