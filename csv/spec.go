package csv

import "fmt"

type Spec struct {
	SkipHeader bool   `json:"skip_header,omitempty"`
	Delimiter  string `json:"delimiter,omitempty"`
}

func (s *Spec) SetDefaults() {
	if s.Delimiter == "" {
		s.Delimiter = ","
	}
}

func (s *Spec) Validate() error {
	if len(s.Delimiter) != 1 {
		return fmt.Errorf("delimiter must be a single character")
	}
	return nil
}
