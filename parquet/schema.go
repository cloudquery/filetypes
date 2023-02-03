package parquet

import (
	"encoding/json"
	"strings"

	"github.com/cloudquery/plugin-sdk/schema"
	pschema "github.com/xitongsys/parquet-go/schema"
)

func (c *Client) makeSchema(cols schema.ColumnList) string {
	s := pschema.JSONSchemaItemType{
		Tag: `name=parquet_go_root, repetitiontype=REQUIRED`,
	}

	for i := range cols {
		tag := `name=` + cols[i].Name
		if opts := c.structOptsForColumn(cols[i]); len(opts) > 0 {
			tag += ", " + strings.Join(opts, ", ")
		}
		s.Fields = append(s.Fields, &pschema.JSONSchemaItemType{Tag: tag})
	}

	b, _ := json.Marshal(s)
	return string(b)
}

func (c *Client) structOptsForColumn(col schema.Column) []string {
	//opts := []string{c.spec.Compression} // TODO fix
	opts := []string{}

	switch col.Type {
	case schema.TypeJSON:
		opts = append(opts, "type=BYTE_ARRAY", "convertedtype=UTF8")
	case schema.TypeTimestamp:
		opts = append(opts, "type=INT64", "convertedtype=TIMESTAMP_MILLIS")
	case schema.TypeString, schema.TypeUUID, schema.TypeCIDR, schema.TypeInet, schema.TypeMacAddr,
		schema.TypeStringArray, schema.TypeUUIDArray, schema.TypeCIDRArray, schema.TypeInetArray, schema.TypeMacAddrArray:
		opts = append(opts, "type=BYTE_ARRAY", "convertedtype=UTF8")
	case schema.TypeFloat:
		opts = append(opts, "type=DOUBLE")
	case schema.TypeInt, schema.TypeIntArray:
		opts = append(opts, "type=INT64")
	case schema.TypeByteArray:
		opts = append(opts, "type=BYTE_ARRAY")
	case schema.TypeBool:
		opts = append(opts, "type=BOOLEAN")
	default:
		panic("unhandled type: " + col.Type.String())
	}

	switch col.Type {
	case schema.TypeStringArray, schema.TypeIntArray, schema.TypeUUIDArray, schema.TypeCIDRArray, schema.TypeInetArray, schema.TypeMacAddrArray:
		opts = append(opts, "repetitiontype=REPEATED")
	default:
		if col.CreationOptions.PrimaryKey || col.CreationOptions.IncrementalKey {
			opts = append(opts, "repetitiontype=REQUIRED")
		} else {
			opts = append(opts, "repetitiontype=OPTIONAL")
		}
	}

	return opts
}
