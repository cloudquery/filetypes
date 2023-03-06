package cqarrow

import (
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/goccy/go-json"
)

const (
	MetadataPrimaryKey     = "cq:extension:primary_key"
	MetadataPrimaryKeyTrue = "true"
)

func CQColumnToArrowField(col *schema.Column) arrow.Field {
	var typ arrow.DataType
	metadata := make(map[string]string)

	switch col.Type {
	case schema.TypeBool:
		typ = arrow.FixedWidthTypes.Boolean
	case schema.TypeInt:
		typ = arrow.PrimitiveTypes.Int64
	case schema.TypeFloat:
		typ = arrow.PrimitiveTypes.Float64
	case schema.TypeUUID:
		typ = NewUUIDType()
	case schema.TypeString:
		typ = arrow.BinaryTypes.String
	case schema.TypeByteArray:
		typ = arrow.BinaryTypes.Binary
	case schema.TypeStringArray:
		typ = arrow.ListOf(arrow.BinaryTypes.String)
	case schema.TypeIntArray:
		typ = arrow.ListOf(arrow.PrimitiveTypes.Int64)
	case schema.TypeTimestamp:
		typ = arrow.FixedWidthTypes.Timestamp_us
	case schema.TypeJSON:
		typ = NewJSONType()
	case schema.TypeUUIDArray:
		typ = arrow.ListOf(NewUUIDType())
	case schema.TypeInet:
		typ = NewInetType()
	case schema.TypeInetArray:
		typ = arrow.ListOf(NewInetType())
	case schema.TypeCIDR:
		typ = NewInetType()
	case schema.TypeCIDRArray:
		typ = arrow.ListOf(NewInetType())
	case schema.TypeMacAddr:
		typ = NewMacType()
	case schema.TypeMacAddrArray:
		typ = arrow.ListOf(NewMacType())
	default:
		panic("unknown type " + typ.Name())
	}
	if col.CreationOptions.PrimaryKey {
		metadata[MetadataPrimaryKey] = MetadataPrimaryKeyTrue
	}
	return arrow.Field{
		Name:     col.Name,
		Type:     typ,
		Nullable: !col.CreationOptions.NotNull,
		Metadata: arrow.MetadataFrom(metadata),
	}
}

func CQSchemaToArrow(table *schema.Table) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(table.Columns))
	for _, col := range table.Columns {
		fields = append(fields, CQColumnToArrowField(&col))
	}
	return arrow.NewSchema(fields, nil)
}

func CqTypesToRecord(mem memory.Allocator, c []schema.CQTypes, arrowSchema *arrow.Schema) arrow.Record {
	bldr := array.NewRecordBuilder(mem, arrowSchema)
	fields := bldr.Fields()
	for i := range fields {
		for j := range c {
			switch c[j][i].Type() {
			case schema.TypeBool:
				bldr.Field(i).(*array.BooleanBuilder).Append(c[j][i].(*schema.Bool).Bool)
			case schema.TypeInt:
				bldr.Field(i).(*array.Int64Builder).Append(c[j][i].(*schema.Int8).Int)
			case schema.TypeFloat:
				bldr.Field(i).(*array.Float64Builder).Append(c[j][i].(*schema.Float8).Float)
			case schema.TypeString:
				bldr.Field(i).(*array.StringBuilder).Append(c[j][i].(*schema.Text).Str)
			case schema.TypeByteArray:
				bldr.Field(i).(*array.BinaryBuilder).Append(c[j][i].(*schema.Bytea).Bytes)
			case schema.TypeStringArray:
				listBldr := bldr.Field(i).(*array.ListBuilder)
				listBldr.Append(true)
				for _, str := range c[j][i].(*schema.TextArray).Elements {
					listBldr.ValueBuilder().(*array.StringBuilder).Append(str.Str)
				}
			case schema.TypeIntArray:
				listBldr := bldr.Field(i).(*array.ListBuilder)
				listBldr.Append(true)
				for _, e := range c[j][i].(*schema.Int8Array).Elements {
					listBldr.ValueBuilder().(*array.Int64Builder).Append(e.Int)
				}
			case schema.TypeTimestamp:
				bldr.Field(i).(*array.TimestampBuilder).Append(arrow.Timestamp(c[j][i].(*schema.Timestamptz).Time.UnixMicro()))
			case schema.TypeJSON:
				var d any
				if err := json.Unmarshal(c[j][i].(*schema.JSON).Bytes, &d); err != nil {
					panic(err)
				}
				bldr.Field(i).(*JSONBuilder).Append(d)
			case schema.TypeUUID:
				bldr.Field(i).(*UUIDBuilder).Append(c[j][i].(*schema.UUID).Bytes)
			case schema.TypeUUIDArray:
				listBldr := bldr.Field(i).(*array.ListBuilder)
				listBldr.Append(true)
				for _, e := range c[j][i].(*schema.UUIDArray).Elements {
					listBldr.ValueBuilder().(*UUIDBuilder).Append(e.Bytes)
				}
			case schema.TypeInet:
				bldr.Field(i).(*InetBuilder).Append(*c[j][i].(*schema.Inet).IPNet)
			case schema.TypeInetArray:
				listBldr := bldr.Field(i).(*array.ListBuilder)
				listBldr.Append(true)
				for _, e := range c[j][i].(*schema.InetArray).Elements {
					listBldr.ValueBuilder().(*InetBuilder).Append(*e.IPNet)
				}
			case schema.TypeCIDR:
				bldr.Field(i).(*InetBuilder).Append(*c[j][i].(*schema.CIDR).IPNet)
			case schema.TypeCIDRArray:
				listBldr := bldr.Field(i).(*array.ListBuilder)
				listBldr.Append(true)
				for _, e := range c[j][i].(*schema.CIDRArray).Elements {
					listBldr.ValueBuilder().(*InetBuilder).Append(*e.IPNet)
				}
			case schema.TypeMacAddr:
				bldr.Field(i).(*MacBuilder).Append(c[j][i].(*schema.Macaddr).Addr)
			case schema.TypeMacAddrArray:
				listBldr := bldr.Field(i).(*array.ListBuilder)
				listBldr.Append(true)
				for _, e := range c[j][i].(*schema.MacaddrArray).Elements {
					listBldr.ValueBuilder().(*MacBuilder).Append(e.Addr)
				}
			}
		}
	}

	return bldr.NewRecord()
}
