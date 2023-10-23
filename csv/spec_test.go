package csv

import (
	"testing"

	"github.com/cloudquery/codegen/jsonschema"
	"github.com/stretchr/testify/require"
)

func TestSpec_JSONSchema(t *testing.T) {
	schema, err := jsonschema.Generate(Spec{})
	require.NoError(t, err)

	jsonschema.TestJSONSchema(t, string(schema), []jsonschema.TestCase{
		{
			Name: "empty",
			Spec: `{}`,
		},
		{
			Name: "extra keyword",
			Err:  true,
			Spec: `{"extra":true}`,
		},
		{
			Name: "skip_header:true",
			Spec: `{"skip_header":true}`,
		},
		{
			Name: "skip_header:false",
			Spec: `{"skip_header":false}`,
		},
		{
			Name: "null skip_header",
			Err:  true,
			Spec: `{"skip_header":null}`,
		},
		{
			Name: "bad skip_header",
			Err:  true,
			Spec: `{"skip_header":123}`,
		},
		{
			Name: "empty delimiter",
			Err:  true,
			Spec: `{"delimiter":""}`,
		},
		{
			Name: "null delimiter",
			Err:  true,
			Spec: `{"delimiter":null}`,
		},
		{
			Name: "bad delimiter",
			Err:  true,
			Spec: `{"delimiter":123}`,
		},
		{
			Name: "delimiter:\",,\"",
			Err:  true,
			Spec: `{"delimiter":",,"}`,
		},
		{
			Name: "tab delimiter",
			Spec: `{"delimiter":"\t"}`,
		},
		{
			Name: "space delimiter",
			Spec: `{"delimiter":" "}`,
		},
	})
}
