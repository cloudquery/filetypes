package cqarrow

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/cloudquery/plugin-sdk/testdata"
	"github.com/google/go-cmp/cmp"
)

func DiffSchema(sc, o *arrow.Schema) (string, bool) {
	switch {
	case sc == o:
		return "", true
	case sc == nil || o == nil:
		return "one of the objects is nil", false
	case len(sc.Fields()) != len(o.Fields()):
		return "diff len", false
	case sc.Endianness() != o.Endianness():
		return "Diff endianness", false
	}

	for i := range sc.Fields() {
		f1 := sc.Fields()[i]
		o1 := o.Fields()[i]
		if !f1.Equal(o1) {
			return fmt.Sprintf("diff at %d", i), false
		}
	}
	return "", true
}

func TestCQSchemaToArrow(t *testing.T) {
	expecetdSchema := arrow.NewSchema([]arrow.Field{
		{Name: "_cq_id", Type: NewUUIDType()},
		{Name: "_cq_parent_id", Type: NewUUIDType(), Nullable: true},
		{Name: "bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "uuid", Type: NewUUIDType(), Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				MetadataPrimaryKey: MetadataPrimaryKeyTrue,
			})},
		{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "text_with_null", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "bytea", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "text_array", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "text_array_with_null", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "int_array", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
		{Name: "json", Type: NewJSONType(), Nullable: true},
		{Name: "uuid_array", Type: arrow.ListOf(NewUUIDType()), Nullable: true},
		{Name: "inet", Type: NewInetType(), Nullable: true},
		{Name: "inet_array", Type: arrow.ListOf(NewInetType()), Nullable: true},
		{Name: "cidr", Type: NewInetType(), Nullable: true},
		{Name: "cidr_array", Type: arrow.ListOf(NewInetType()), Nullable: true},
		{Name: "macaddr", Type: NewMacType(), Nullable: true},
		{Name: "macaddr_array", Type: arrow.ListOf(NewMacType()), Nullable: true},
	}, nil)

	testTable := testdata.TestSourceTable("test_table")
	arrowSchema := CQSchemaToArrow(testTable)
	if diff := cmp.Diff(arrowSchema.String(), expecetdSchema.String()); diff != "" {
		t.Errorf(diff)
	}
	// if diff, _ := DiffSchema(arrowSchema, expecetdSchema); diff != "" {
	// 	t.Errorf(diff)
	// }
	if !arrowSchema.Equal(expecetdSchema) {
		t.Errorf("got:\n%v\nwant:\n%v\n", arrowSchema, expecetdSchema)
	}
}

func TestCQTypesToRecord(t *testing.T) {
	testTable := testdata.TestSourceTable("test_table")
	testCqTypes := testdata.GenTestData(testTable)
	arrowSchema := CQSchemaToArrow(testTable)
	mem := memory.NewGoAllocator()
	record := CqTypesToRecord(mem, []schema.CQTypes{testCqTypes}, arrowSchema)
	str, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		t.Error(err)
	}
	t.Log(string(str))
}
