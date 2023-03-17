package csv

import (
	"github.com/cloudquery/plugin-sdk/plugins/destination"
	"github.com/cloudquery/plugin-sdk/schema"
)

type ReverseTransformer struct {
	*destination.DefaultReverseTransformer
}

var _ interface {
	ReverseTransformValues(table *schema.Table, values []any) (schema.CQTypes, error)
} = ReverseTransformer{}
