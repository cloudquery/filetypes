package csv

import "fmt"

// nolint:revive
type CSVSpec struct {
	IncludeHeaders bool   `json:"include_headers,omitempty"`
	Delimiter      string `json:"delimiter,omitempty"`
}

func (s *CSVSpec) SetDefaults() {
	if s.Delimiter == "" {
		s.Delimiter = ","
	}
}

func (s *CSVSpec) Validate() error {
	if len(s.Delimiter) != 1 {
		return fmt.Errorf("delimiter must be a single character")
	}
	return nil
}
