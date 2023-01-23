package json

type JSONSpec struct{}

func (s *JSONSpec) SetDefaults() {}

func (*JSONSpec) Validate() error {
	return nil
}
