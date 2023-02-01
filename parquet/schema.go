package parquet

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/segmentio/parquet-go"
)

func (c *Client) makeStruct(cols schema.ColumnList) any {
	sf := make([]reflect.StructField, len(cols))
	for i := range cols {
		sf[i] = c.structFieldFromColumn(i, cols[i])
	}

	return reflect.New(reflect.StructOf(sf)).Elem().Interface()
}

func (c *Client) structFieldFromColumn(index int, col schema.Column) reflect.StructField {
	el := schemaTypeToGoType(col.Type)
	f := reflect.StructField{
		Name: formatFieldNameWithIndex(index),
		Type: reflect.TypeOf(el),
	}

	tg := `parquet:"` + col.Name
	if opts := c.structOptsForColumn(col); len(opts) > 0 {
		tg += "," + strings.Join(opts, ",")
	}
	tg += `"`

	f.Tag = reflect.StructTag(tg)
	return f
}

func (c *Client) structOptsForColumn(col schema.Column) []string {
	opts := []string{c.spec.Compression}

	switch col.Type {
	case schema.TypeJSON:
		opts = append(opts, "json")
	case schema.TypeTimestamp:
		opts = append(opts, "timestamp")
	case schema.TypeUUID, schema.TypeInt, schema.TypeString, schema.TypeByteArray:
		opts = append(opts, "delta")
	case schema.TypeStringArray, schema.TypeIntArray, schema.TypeUUIDArray, schema.TypeCIDRArray, schema.TypeInetArray:
		opts = append(opts, "list")
	}

	if !col.CreationOptions.PrimaryKey && !col.CreationOptions.IncrementalKey {
		opts = append(opts, "optional")
	}

	return opts
}

func formatFieldNameWithIndex(index int) string {
	return "Field" + strconv.FormatInt(int64(index), 10)
}

func schemaTypeToGoType(v schema.ValueType) any {
	switch v {
	// Non-primitive types
	case schema.TypeCIDR, schema.TypeInet:
		return ""
	case schema.TypeUUID:
		return ""
	case schema.TypeMacAddr:
		return ""
	case schema.TypeTimestamp:
		return time.Time{}

	// Map types
	case schema.TypeJSON:
		return ""

	// Slice types
	case schema.TypeStringArray:
		return []string{}
	case schema.TypeIntArray:
		return []int64{}
	case schema.TypeByteArray:
		return []byte{}
	case schema.TypeCIDRArray, schema.TypeInetArray:
		return []string{}
	case schema.TypeUUIDArray:
		return []string{}
	case schema.TypeMacAddrArray:
		return []string{}

	// Primitive types
	case schema.TypeString:
		return ""
	case schema.TypeInt:
		return int64(0)
	case schema.TypeBool:
		return false
	case schema.TypeFloat:
		return float64(0)

	default:
		panic(fmt.Sprintf("unsupported type %s", v))
	}
}

type schemaSetter struct {
	Schema *parquet.Schema
}

func (s schemaSetter) ConfigureWriter(c *parquet.WriterConfig) {
	c.Schema = s.Schema
}

func (s schemaSetter) ConfigureReader(c *parquet.ReaderConfig) {
	c.Schema = s.Schema
}
