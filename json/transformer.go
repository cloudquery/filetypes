package json

import (
	"encoding/json"

	"github.com/cloudquery/plugin-sdk/plugins/destination"
	"github.com/cloudquery/plugin-sdk/schema"
)

type ReverseTransformer struct {
	*destination.DefaultReverseTransformer
}

var _ interface {
	ReverseTransformValues(table *schema.Table, values []any) (schema.CQTypes, error)
} = ReverseTransformer{}

type Transformer struct{}

var _ schema.CQTypeTransformer = Transformer{}

func (Transformer) TransformBool(v *schema.Bool) any {
	if v.Status != schema.Present {
		return nil
	}
	return v.Bool
}

func (Transformer) TransformBytea(v *schema.Bytea) any {
	if v.Status != schema.Present {
		return nil
	}
	return v.String()
}

func (Transformer) TransformFloat8(v *schema.Float8) any {
	if v.Status != schema.Present {
		return nil
	}
	return v.Float
}

func (Transformer) TransformInt8(v *schema.Int8) any {
	if v.Status != schema.Present {
		return nil
	}
	return v.Int
}

func (Transformer) TransformInt8Array(v *schema.Int8Array) any {
	if v.Status != schema.Present {
		return nil
	}
	res := make([]int64, len(v.Elements))
	for i, e := range v.Elements {
		res[i] = e.Int
	}
	return res
}

func (Transformer) TransformJSON(v *schema.JSON) any {
	if v.Status != schema.Present {
		return nil
	}
	var res any
	if err := json.Unmarshal(v.Bytes, &res); err != nil {
		panic(err)
	}
	return res
}

func (Transformer) TransformText(v *schema.Text) any {
	if v.Status != schema.Present {
		return nil
	}
	return v.Str
}

func (Transformer) TransformTextArray(v *schema.TextArray) any {
	if v.Status != schema.Present {
		return nil
	}
	res := make([]string, len(v.Elements))
	for i, e := range v.Elements {
		res[i] = e.Str
	}
	return res
}

func (Transformer) TransformTimestamptz(v *schema.Timestamptz) any {
	if v.Status != schema.Present {
		return nil
	}
	return v.String()
}

func (Transformer) TransformUUID(v *schema.UUID) any {
	if v.Status != schema.Present {
		return nil
	}
	return v.String()
}

func (Transformer) TransformUUIDArray(v *schema.UUIDArray) any {
	if v.Status != schema.Present {
		return nil
	}
	res := make([]string, len(v.Elements))
	for i, e := range v.Elements {
		res[i] = e.String()
	}
	return res
}

func (Transformer) TransformCIDR(v *schema.CIDR) any {
	if v.Status != schema.Present {
		return nil
	}
	return v.String()
}

func (Transformer) TransformCIDRArray(v *schema.CIDRArray) any {
	if v.Status != schema.Present {
		return nil
	}
	res := make([]string, len(v.Elements))
	for i, e := range v.Elements {
		res[i] = e.String()
	}
	return res
}

func (Transformer) TransformInet(v *schema.Inet) any {
	if v.Status != schema.Present {
		return nil
	}
	return v.String()
}

func (Transformer) TransformInetArray(v *schema.InetArray) any {
	if v.Status != schema.Present {
		return nil
	}
	res := make([]string, len(v.Elements))
	for i, e := range v.Elements {
		res[i] = e.String()
	}
	return res
}

func (Transformer) TransformMacaddr(v *schema.Macaddr) any {
	if v.Status != schema.Present {
		return nil
	}
	return v.String()
}

func (Transformer) TransformMacaddrArray(v *schema.MacaddrArray) any {
	if v.Status != schema.Present {
		return nil
	}
	res := make([]string, len(v.Elements))
	for i, e := range v.Elements {
		res[i] = e.String()
	}
	return res
}
