package filetypes

import (
	"reflect"

	cq_jsonschema "github.com/cloudquery/codegen/jsonschema"
	"github.com/cloudquery/filetypes/v4/csv"
	jsonfile "github.com/cloudquery/filetypes/v4/json"
	"github.com/cloudquery/filetypes/v4/parquet"
	"github.com/invopop/jsonschema"
	orderedmap "github.com/wk8/go-ordered-map/v2"
)

// JSONSchemaOptions should be used when generating schema to add the nested spec info
func (FileSpec) JSONSchemaOptions() []cq_jsonschema.Option {
	fileSpecType := reflect.TypeOf(FileSpec{})
	return []cq_jsonschema.Option{func(r *jsonschema.Reflector) {
		fileSpecFields := func(t reflect.Type) []reflect.StructField {
			if t != fileSpecType {
				return nil
			}
			return reflect.VisibleFields(reflect.TypeOf(struct {
				CSVSpec     csv.Spec
				JSONSpec    jsonfile.Spec
				ParquetSpec parquet.Spec
			}{}))
		}
		if r.AdditionalFields == nil {
			r.AdditionalFields = fileSpecFields
		} else {
			old := r.AdditionalFields
			r.AdditionalFields = func(r reflect.Type) []reflect.StructField {
				if extra := fileSpecFields(r); len(extra) > 0 {
					return extra
				}
				return old(r)
			}
		}
	}}
}

func (FileSpec) JSONSchemaExtend(sc *jsonschema.Schema) {
	// now we need to remove extra fields
	refCSVSpec := sc.Properties.Value("CSVSpec").Ref
	refJSONSpec := sc.Properties.Value("JSONSpec").Ref
	refParquetSpec := sc.Properties.Value("ParquetSpec").Ref
	sc.Properties.Delete("CSVSpec")
	sc.Properties.Delete("JSONSpec")
	sc.Properties.Delete("ParquetSpec")

	sc.Properties.Set("format_spec", &jsonschema.Schema{
		OneOf: []*jsonschema.Schema{
			{
				AnyOf: []*jsonschema.Schema{
					{Ref: refCSVSpec},
					{Ref: refJSONSpec},
					{Ref: refParquetSpec},
				},
			},
			{Type: "null"},
		},
	})

	// now we need to enforce format -> specific type
	formatSpecOneOf := []*jsonschema.Schema{
		// CSV
		{
			Properties: func() *orderedmap.OrderedMap[string, *jsonschema.Schema] {
				properties := jsonschema.NewProperties()
				properties.Set("format", &jsonschema.Schema{Type: "string", Const: FormatTypeCSV})
				properties.Set("format_spec", &jsonschema.Schema{
					OneOf: []*jsonschema.Schema{{Ref: refCSVSpec}, {Type: "null"}},
				})
				return properties
			}(),
		},
		// JSON
		{
			Properties: func() *orderedmap.OrderedMap[string, *jsonschema.Schema] {
				properties := jsonschema.NewProperties()
				properties.Set("format", &jsonschema.Schema{Type: "string", Const: FormatTypeJSON})
				properties.Set("format_spec", &jsonschema.Schema{
					OneOf: []*jsonschema.Schema{{Ref: refJSONSpec}, {Type: "null"}},
				})
				return properties
			}(),
		},
		// Parquet
		{
			Properties: func() *orderedmap.OrderedMap[string, *jsonschema.Schema] {
				properties := jsonschema.NewProperties()
				properties.Set("format", &jsonschema.Schema{Type: "string", Const: FormatTypeParquet})
				properties.Set("format_spec", &jsonschema.Schema{
					OneOf: []*jsonschema.Schema{{Ref: refParquetSpec}, {Type: "null"}},
				})
				return properties
			}(),
		},
	}
	if sc.OneOf == nil {
		sc.OneOf = formatSpecOneOf
	} else {
		// may happen when embedding, so move to all_of{{one_of},{one_of}}
		sc.AllOf = []*jsonschema.Schema{
			{OneOf: sc.OneOf},
			{OneOf: formatSpecOneOf},
		}
		sc.OneOf = nil
	}
}
