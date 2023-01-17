package csv

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/cloudquery/plugin-sdk/testdata"
)

func TestWriteRead(t *testing.T) {
	cases := []struct {
		headers     bool
		outputCount int
	}{
		{headers: true, outputCount: 2},
		{headers: false, outputCount: 1},
	}

	for _, tc := range cases {
		var b bytes.Buffer
		table := testdata.TestTable("test")
		cqtypes := testdata.GenTestData(table)
		if err := cqtypes[0].Set("test-source"); err != nil {
			t.Fatal(err)
		}
		writer := bufio.NewWriter(&b)
		reader := bufio.NewReader(&b)
		transformer := &Transformer{}
		transformedValues := schema.TransformWithTransformer(transformer, cqtypes)
		client, err := NewClient(
			WithHeader(tc.headers),
		)
		if err != nil {
			t.Fatal(err)
		}

		if err := client.WriteTableBatch(writer, table, [][]any{transformedValues}); err != nil {
			t.Fatal(err)
		}
		writer.Flush()

		ch := make(chan []any)
		var readErr error
		go func() {
			readErr = client.Read(reader, table, ch)
			close(ch)
		}()
		totalCount := 0
		reverseTransformer := &ReverseTransformer{}
		for row := range ch {
			if tc.headers && totalCount == 0 {
				totalCount++
				continue
			}
			gotCqtypes, err := reverseTransformer.ReverseTransformValues(table, row)
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
		if totalCount != tc.outputCount {
			t.Fatalf("expected %d row, got %d", tc.outputCount, totalCount)
		}
	}
}
