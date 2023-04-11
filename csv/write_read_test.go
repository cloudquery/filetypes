package csv

import (
	"bytes"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/cloudquery/plugin-sdk/v2/plugins/destination"
	"github.com/cloudquery/plugin-sdk/v2/testdata"
	"github.com/google/uuid"
)

func TestWriteRead(t *testing.T) {
	cases := []struct {
		name        string
		options     []Options
		outputCount int
	}{
		{name: "default", outputCount: 1},
		{name: "with_headers", options: []Options{WithHeader()}, outputCount: 1},
		{name: "with_delimiter", options: []Options{WithDelimiter('\t')}, outputCount: 1},
		{name: "with_delimiter_headers", options: []Options{WithDelimiter('\t'), WithHeader()}, outputCount: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var b bytes.Buffer
			table := testdata.TestTable("test")
			arrowSchema := table.ToArrowSchema()
			sourceName := "test-source"
			syncTime := time.Now().UTC().Round(1 * time.Second)
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)
			opts := testdata.GenTestDataOptions{
				SourceName: sourceName,
				SyncTime:   syncTime,
				MaxRows:    1,
				StableUUID: uuid.Nil,
			}
			records := testdata.GenTestData(mem, arrowSchema, opts)
			defer func() {
				for _, r := range records {
					r.Release()
				}
			}()
			cl, err := NewClient(tc.options...)
			if err != nil {
				t.Fatal(err)
			}
			if err := cl.WriteTableBatch(&b, arrowSchema, records); err != nil {
				t.Fatal(err)
			}

			ch := make(chan arrow.Record)
			var readErr error
			go func() {
				readErr = cl.Read(&b, arrowSchema, "test-source", ch)
				close(ch)
			}()
			totalCount := 0
			for got := range ch {
				if diff := destination.RecordDiff(records[totalCount], got); diff != "" {
					got.Release()
					t.Fatalf("got diff: %s", diff)
				}
				got.Release()
				totalCount++
			}
			if readErr != nil {
				t.Fatal(readErr)
			}
			if totalCount != tc.outputCount {
				t.Fatalf("got %d row(s), want %d", totalCount, tc.outputCount)
			}
		})
	}
}
