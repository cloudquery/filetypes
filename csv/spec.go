package csv

type CSVSpec struct {
	IncludeHeaders bool   `json:"include_headers,omitempty"`
	Delimiter      string `json:"delimiter,omitempty"`
}

func (s *CSVSpec) SetDefaults() {}

func (*CSVSpec) Validate() error {
	return nil
}
