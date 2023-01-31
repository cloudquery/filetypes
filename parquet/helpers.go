package parquet

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/segmentio/parquet-go"
)

func makeStruct(cols schema.ColumnList) any {
	sf := make([]reflect.StructField, len(cols))
	for i := range cols {
		sf[i] = structFieldFromColumn(i, cols[i])
	}

	return reflect.New(reflect.StructOf(sf)).Elem().Interface()
}

func formatFieldNameWithIndex(index int) string {
	return "Field" + strconv.FormatInt(int64(index), 10)
}

func structFieldFromColumn(index int, c schema.Column) reflect.StructField {
	el := schemaTypeToGoType(c.Type)
	f := reflect.StructField{
		Name: formatFieldNameWithIndex(index),
		Type: reflect.TypeOf(el),
	}

	tg := `parquet:"` + c.Name
	if !c.CreationOptions.PrimaryKey && !c.CreationOptions.IncrementalKey {
		tg += `,optional"`
	}
	tg += `"`

	f.Tag = reflect.StructTag(tg)
	return f
}

func schemaTypeToGoType(v schema.ValueType) any {
	switch v {
	// Non-primitive types
	case schema.TypeCIDR, schema.TypeInet:
		//return &net.IPNet{}
		return ""
	case schema.TypeUUID:
		return [16]byte{}
	case schema.TypeMacAddr:
		return ""
	case schema.TypeTimestamp:
		return time.Time{}

	// Map types
	case schema.TypeJSON:
		//return map[string]any{} // TODO fix
		return ""

	// Slice types
	case schema.TypeStringArray:
		return []string{}
	case schema.TypeIntArray:
		return []int64{}
	case schema.TypeByteArray:
		return []byte{}
	case schema.TypeCIDRArray, schema.TypeInetArray:
		//return []*net.IPNet{}
		return []string{}
	case schema.TypeUUIDArray:
		return [][16]byte{}
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
