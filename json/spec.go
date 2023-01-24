package json

type Spec struct{}

func (*Spec) SetDefaults() {}

func (*Spec) Validate() error {
	return nil
}
