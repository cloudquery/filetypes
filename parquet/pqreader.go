package parquet

import (
	"bytes"
	"fmt"

	"github.com/xitongsys/parquet-go/source"
)

type pqReader struct {
	data []byte

	*bytes.Reader
}

var _ source.ParquetFile = (*pqReader)(nil)

func newPQReader(data []byte) *pqReader {
	bu := make([]byte, len(data))
	copy(bu, data)

	return &pqReader{
		Reader: bytes.NewReader(bu),
		data:   bu,
	}
}
func (pq *pqReader) Open(string) (source.ParquetFile, error) {
	return newPQReader(pq.data), nil
}

func (*pqReader) Close() error {
	return nil
}

func (*pqReader) Write([]byte) (n int, err error) {
	return 0, fmt.Errorf("not implemented")
}

func (*pqReader) Create(string) (source.ParquetFile, error) {
	return nil, fmt.Errorf("not implemented")
}
