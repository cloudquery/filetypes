package parquet

import (
	"encoding/json"
	"strings"

	"github.com/cloudquery/plugin-sdk/schema"
	pschema "github.com/xitongsys/parquet-go/schema"
)

func makeSchema(tableName string, cols schema.ColumnList) string {
	s := pschema.JSONSchemaItemType{
		Tag: `name=` + tableName + `_root, repetitiontype=REQUIRED`,
	}

	for _, col := range cols {
		var subFields []*pschema.JSONSchemaItemType

		tag := []string{`name=` + col.Name}

		switch col.Type {
		case schema.TypeTimestamp:
			tag = append(tag, "type=INT64", "convertedtype=TIMESTAMP_MILLIS")
		case schema.TypeJSON, schema.TypeString, schema.TypeUUID, schema.TypeCIDR, schema.TypeInet, schema.TypeMacAddr:
			tag = append(tag, "type=BYTE_ARRAY", "convertedtype=UTF8")
		case schema.TypeFloat:
			tag = append(tag, "type=DOUBLE")
		case schema.TypeInt:
			tag = append(tag, "type=INT64")
		case schema.TypeByteArray:
			tag = append(tag, "type=BYTE_ARRAY")
		case schema.TypeBool:
			tag = append(tag, "type=BOOLEAN")
		case schema.TypeIntArray:
			tag = append(tag, "type=LIST", "repetitiontype=OPTIONAL")
			subFields = []*pschema.JSONSchemaItemType{
				{
					Tag: "name=element, type=INT64, repetitiontype=OPTIONAL",
				},
			}
		case schema.TypeStringArray, schema.TypeUUIDArray, schema.TypeCIDRArray, schema.TypeInetArray, schema.TypeMacAddrArray:
			tag = append(tag, "type=LIST", "repetitiontype=OPTIONAL")
			subFields = []*pschema.JSONSchemaItemType{
				{
					Tag: "name=element, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL",
				},
			}
		default:
			panic("unhandled type: " + col.Type.String())
		}

		if !isArray(col.Type) { // array types are handled differently, see above
			if col.CreationOptions.PrimaryKey || col.CreationOptions.IncrementalKey {
				tag = append(tag, "repetitiontype=REQUIRED")
			} else {
				tag = append(tag, "repetitiontype=OPTIONAL")
			}
		}

		s.Fields = append(s.Fields, &pschema.JSONSchemaItemType{
			Tag:    strings.Join(tag, ", "),
			Fields: subFields,
		})
	}

	b, _ := json.Marshal(s)
	return string(b)
}

func isArray(t schema.ValueType) bool {
	return arrayElement(t) != schema.TypeInvalid
}

func arrayElement(t schema.ValueType) schema.ValueType {
	switch t {
	case schema.TypeIntArray:
		return schema.TypeInt
	case schema.TypeStringArray:
		return schema.TypeString
	case schema.TypeUUIDArray:
		return schema.TypeUUID
	case schema.TypeCIDRArray:
		return schema.TypeCIDR
	case schema.TypeInetArray:
		return schema.TypeInet
	case schema.TypeMacAddrArray:
		return schema.TypeMacAddr
	default:
		return schema.TypeInvalid
	}
}
