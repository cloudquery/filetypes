package csv

import (
	"bufio"
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/bradleyjkemp/cupaloy/v2"
	"github.com/cloudquery/plugin-sdk/v3/plugins/destination"
	"github.com/cloudquery/plugin-sdk/v3/schema"
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
			table := schema.TestTable("test")
			sourceName := "test-source"
			syncTime := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
			opts := schema.GenTestDataOptions{
				SourceName: sourceName,
				SyncTime:   syncTime,
				MaxRows:    2,
				StableUUID: uuid.MustParse("00000000-0000-0000-0000-000000000001"),
				StableTime: time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
			}
			records := schema.GenTestData(table, opts)
			cl, err := NewClient(tc.options...)
			if err != nil {
				t.Fatal(err)
			}

			var b bytes.Buffer
			writer := bufio.NewWriter(&b)
			reader := bufio.NewReader(&b)

			if err := cl.WriteTableBatch(writer, table, records); err != nil {
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
				readErr = cl.Read(byteReader, table, "test-source", ch)
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

func BenchmarkWrite(b *testing.B) {
	table := schema.TestTable("test")
	sourceName := "test-source"
	syncTime := time.Now().UTC().Round(1 * time.Second)
	opts := schema.GenTestDataOptions{
		SourceName: sourceName,
		SyncTime:   syncTime,
		MaxRows:    1000,
	}
	records := schema.GenTestData(table, opts)

	cl, err := NewClient()
	if err != nil {
		b.Fatal(err)
	}
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cl.WriteTableBatch(writer, table, records); err != nil {
			b.Fatal(err)
		}
		err = writer.Flush()
		if err != nil {
			b.Fatal(err)
		}
		buf.Reset()
	}
}
