package parquet

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/cloudquery/plugin-sdk/testdata"
)

func TestWriteRead(t *testing.T) {
	var b bytes.Buffer
	table := testdata.TestTable("test")
	cqtypes := testdata.GenTestData(table)
	if err := cqtypes[0].Set("test-source"); err != nil {
		t.Fatal(err)
	}
	transformer := &Transformer{}
	transformedValues := schema.TransformWithTransformer(transformer, cqtypes)

	writer := bufio.NewWriter(&b)
	reader := bufio.NewReader(&b)

	cl, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.WriteTableBatch(writer, table, [][]any{transformedValues}); err != nil {
		t.Fatal(err)
	}
	writer.Flush()

	ch := make(chan []any)
	var readErr error
	go func() {
		readErr = cl.Read(reader, table, "test-source", ch)
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
