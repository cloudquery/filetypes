package parquet

import (
	"bufio"
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

func TestWriteRead(t *testing.T) {
	var b bytes.Buffer
	table := schema.TestTable("test", schema.TestSourceOptions{})
	sourceName := "test-source"
	syncTime := time.Now().UTC().Round(time.Second)
	opts := schema.GenTestDataOptions{
		SourceName: sourceName,
		SyncTime:   syncTime,
		MaxRows:    2,
	}
	tg := schema.NewTestDataGenerator()
	records := tg.Generate(table, opts)

	writer := bufio.NewWriter(&b)
	reader := bufio.NewReader(&b)

	cl, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	if err := types.WriteAll(cl, writer, table, records); err != nil {
		t.Fatal(err)
	}
	err = writer.Flush()
	if err != nil {
		t.Fatal(err)
	}

	rawBytes, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	byteReader := bytes.NewReader(rawBytes)
	ch := make(chan arrow.Record)
	var readErr error
	go func() {
		readErr = cl.Read(byteReader, table, ch)
		close(ch)
	}()
	totalCount := 0
	for got := range ch {
		curr := records[totalCount]
		if !array.RecordApproxEqual(curr, got) {
			t.Fatalf("got diff (record %d): %s\n", totalCount, plugin.RecordDiff(records[totalCount], got))
		}
		totalCount++
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
	if totalCount != 2 {
		t.Fatalf("expected 2 rows, got %d", totalCount)
	}
}

func BenchmarkWrite(b *testing.B) {
	table := schema.TestTable("test", schema.TestSourceOptions{})
	sourceName := "test-source"
	syncTime := time.Now().UTC().Round(time.Second)
	opts := schema.GenTestDataOptions{
		SourceName: sourceName,
		SyncTime:   syncTime,
		MaxRows:    b.N,
	}
	tg := schema.NewTestDataGenerator()
	records := tg.Generate(table, opts)

	cl, err := NewClient()
	if err != nil {
		b.Fatal(err)
	}
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	b.ResetTimer()

	if err := types.WriteAll(cl, writer, table, records); err != nil {
		b.Fatal(err)
	}
	err = writer.Flush()
	if err != nil {
		b.Fatal(err)
	}
}
