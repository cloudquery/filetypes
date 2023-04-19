package csv

import (
	"bufio"
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/bradleyjkemp/cupaloy/v2"
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
		{name: "default", outputCount: 2},
		{name: "with_headers", options: []Options{WithHeader()}, outputCount: 2},
		{name: "with_delimiter", options: []Options{WithDelimiter('\t')}, outputCount: 2},
		{name: "with_delimiter_headers", options: []Options{WithDelimiter('\t'), WithHeader()}, outputCount: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := testdata.TestTable("test")
			arrowSchema := table.ToArrowSchema()
			sourceName := "test-source"
			syncTime := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
			mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer mem.AssertSize(t, 0)
			opts := testdata.GenTestDataOptions{
				SourceName: sourceName,
				SyncTime:   syncTime,
				MaxRows:    2,
				StableUUID: uuid.MustParse("00000000-0000-0000-0000-000000000001"),
				StableTime: time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
			}
			records := testdata.GenTestData(mem, arrowSchema, opts)
			for _, r := range records {
				r.Retain()
			}
			defer func() {
				for _, r := range records {
					r.Release()
				}
			}()
			cl, err := NewClient(tc.options...)
			if err != nil {
				t.Fatal(err)
			}

			var b bytes.Buffer
			writer := bufio.NewWriter(&b)
			reader := bufio.NewReader(&b)

			if err := cl.WriteTableBatch(writer, arrowSchema, records); err != nil {
				t.Fatal(err)
			}
			writer.Flush()

			rawBytes, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			snap := cupaloy.New(
				cupaloy.SnapshotFileExtension(".csv"),
				cupaloy.SnapshotSubdirectory("testdata"),
			)
			snap.SnapshotT(t, string(rawBytes))

			byteReader := bytes.NewReader(rawBytes)

			ch := make(chan arrow.Record)
			var readErr error
			go func() {
				readErr = cl.Read(byteReader, arrowSchema, "test-source", ch)
				close(ch)
			}()
			totalCount := 0
			for got := range ch {
				if diff := destination.RecordDiff(records[totalCount], got); diff != "" {
					t.Errorf("got diff: %s", diff)
				}
				totalCount++
			}
			if readErr != nil {
				t.Fatal(readErr)
			}
			if totalCount != tc.outputCount {
				t.Errorf("got %d row(s), want %d", totalCount, tc.outputCount)
			}
		})
	}
}
