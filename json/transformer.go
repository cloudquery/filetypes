package json

import (
	"github.com/cloudquery/plugin-sdk/v2/plugins/destination"
	"github.com/cloudquery/plugin-sdk/v2/schema"
)

type ReverseTransformer struct {
	*destination.DefaultReverseTransformer
}

var _ interface {
	ReverseTransformValues(table *schema.Table, values []any) (schema.CQTypes, error)
} = ReverseTransformer{}
