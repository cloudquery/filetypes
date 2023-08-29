package parquet

import (
	"bufio"
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/stretchr/testify/require"
)

func TestWriteRead(t *testing.T) {
	const rows = 10
	var b bytes.Buffer
	table := schema.TestTable("test", schema.TestSourceOptions{})
	sourceName := "test-source"
	syncTime := time.Now().UTC().Round(time.Second)
	opts := schema.GenTestDataOptions{
		SourceName: sourceName,
		SyncTime:   syncTime,
		MaxRows:    rows,
	}
	tg := schema.NewTestDataGenerator()
	record := tg.Generate(table, opts)

	writer := bufio.NewWriter(&b)
	reader := bufio.NewReader(&b)

	cl, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	if err := types.WriteAll(cl, writer, table, []arrow.Record{record}); err != nil {
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
	received := make([]arrow.Record, 0, rows)
	for got := range ch {
		received = append(received, got)
	}
	require.Empty(t, plugin.RecordsDiff(table.ToArrowSchema(), []arrow.Record{record}, received))
	require.NoError(t, readErr)
	require.Equalf(t, rows, len(received), "got %d row(s), want %d", len(received), rows)
}
func TestWriteReadSliced(t *testing.T) {
	const rows = 10
	var b bytes.Buffer
	table := schema.TestTable("test", schema.TestSourceOptions{})
	sourceName := "test-source"
	syncTime := time.Now().UTC().Round(time.Second)
	opts := schema.GenTestDataOptions{
		SourceName: sourceName,
		SyncTime:   syncTime,
		MaxRows:    rows,
	}
	tg := schema.NewTestDataGenerator()
	record := tg.Generate(table, opts)

	writer := bufio.NewWriter(&b)
	reader := bufio.NewReader(&b)

	cl, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	if err := types.WriteAll(cl, writer, table, slice(record)); err != nil {
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
	received := make([]arrow.Record, 0, rows)
	for got := range ch {
		received = append(received, got)
	}
	require.Empty(t, plugin.RecordsDiff(table.ToArrowSchema(), []arrow.Record{record}, received))
	require.NoError(t, readErr)
	require.Equalf(t, rows, len(received), "got %d row(s), want %d", len(received), rows)
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
	record := tg.Generate(table, opts)

	cl, err := NewClient()
	if err != nil {
		b.Fatal(err)
	}
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	b.ResetTimer()

	if err := types.WriteAll(cl, writer, table, []arrow.Record{record}); err != nil {
		b.Fatal(err)
	}
	err = writer.Flush()
	if err != nil {
		b.Fatal(err)
	}
}
