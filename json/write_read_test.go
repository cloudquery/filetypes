package json

import (
	"bytes"
	"testing"

	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/cloudquery/plugin-sdk/testdata"
)

func GenTestSourceTable(name string) *schema.Table {
	return &schema.Table{
		Name:        name,
		Description: "Test table",
		Columns: schema.ColumnList{
			schema.CqSourceNameColumn,
			schema.CqIDColumn,
			schema.CqParentIDColumn,
			{
				Name: "bool",
				Type: schema.TypeBool,
			},
			{
				Name: "int",
				Type: schema.TypeInt,
			},
			{
				Name: "float",
				Type: schema.TypeFloat,
			},
			{
				Name:            "uuid",
				Type:            schema.TypeUUID,
				CreationOptions: schema.ColumnCreationOptions{PrimaryKey: true},
			},
			{
				Name: "text",
				Type: schema.TypeString,
			},
			{
				Name: "text_with_null",
				Type: schema.TypeString,
			},
			// {
			// 	Name: "bytea",
			// 	Type: schema.TypeByteArray,
			// },
			{
				Name: "text_array",
				Type: schema.TypeStringArray,
			},
			{
				Name: "text_array_with_null",
				Type: schema.TypeStringArray,
			},
			{
				Name: "int_array",
				Type: schema.TypeIntArray,
			},
			// {
			// 	Name: "timestamp",
			// 	Type: schema.TypeTimestamp,
			// },
			{
				Name: "json",
				Type: schema.TypeJSON,
			},
			{
				Name: "uuid_array",
				Type: schema.TypeUUIDArray,
			},
			{
				Name: "inet",
				Type: schema.TypeInet,
			},
			{
				Name: "inet_array",
				Type: schema.TypeInetArray,
			},
			{
				Name: "cidr",
				Type: schema.TypeCIDR,
			},
			{
				Name: "cidr_array",
				Type: schema.TypeCIDRArray,
			},
			{
				Name: "macaddr",
				Type: schema.TypeMacAddr,
			},
			{
				Name: "macaddr_array",
				Type: schema.TypeMacAddrArray,
			},
		},
	}
}

func TestWriteRead(t *testing.T) {
	var b bytes.Buffer
	// table := testdata.TestTable("test")
	table := GenTestSourceTable("test")
	cqtypes := testdata.GenTestData(table)
	if err := cqtypes[0].Set("test-source"); err != nil {
		t.Fatal(err)
	}
	transformer := &schema.DefaultTransformer{}
	transformedValues := schema.TransformWithTransformer(transformer, cqtypes)
	// expectedRecord := cqarrow.CqTypesToRecord(memory.DefaultAllocator, []schema.CQTypes{cqtypes}, cqarrow.CQSchemaToArrow(table))

	cl, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.WriteTableBatch(&b, table, [][]any{transformedValues}); err != nil {
		t.Fatal(err)
	}

	ch := make(chan []any)
	var readErr error
	go func() {
		readErr = cl.Read(&b, table, "test-source", ch)
		close(ch)
	}()
	totalCount := 0
	reverseTransformer := &ReverseTransformer{}
	for resource := range ch {
		gotCqtypes, err := reverseTransformer.ReverseTransformValues(table, resource)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cqtypes.Diff(gotCqtypes); diff != "" {
			t.Fatalf("got diff: %s", diff)
		}
		totalCount++
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
	if totalCount != 1 {
		t.Fatalf("expected 1 row, got %d", totalCount)
	}
}
