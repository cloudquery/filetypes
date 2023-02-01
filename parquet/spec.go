package parquet

import "fmt"

type Spec struct {
	Compression string `json:"compression,omitempty"`
}

func (s *Spec) SetDefaults() {
	if s.Compression == "" {
		s.Compression = "snappy"
	}
}

func (s *Spec) Validate() error {
	if s.Compression != "snappy" &&
		s.Compression != "gzip" &&
		s.Compression != "brotli" &&
		s.Compression != "lz4" &&
		s.Compression != "zstd" &&
		s.Compression != "uncompressed" {
		return fmt.Errorf("compression must be one of snappy, gzip, brotli, lz4, zstd, or uncompressed")
	}
	return nil
}
