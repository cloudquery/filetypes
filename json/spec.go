package json

//nolint:revive
type JSONSpec struct{}

func (*JSONSpec) SetDefaults() {}

func (*JSONSpec) Validate() error {
	return nil
}
