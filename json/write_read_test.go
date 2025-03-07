package json

import (
	"bufio"
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/bradleyjkemp/cupaloy/v2"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/stretchr/testify/require"
)

func TestWriteRead(t *testing.T) {
	table := schema.TestTable("test", schema.TestSourceOptions{})
	sourceName := "test-source"
	syncTime := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	opts := schema.GenTestDataOptions{
		SourceName: sourceName,
		SyncTime:   syncTime,
		MaxRows:    2,
		StableTime: time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	tg := schema.NewTestDataGenerator(0)
	record := tg.Generate(table, opts)

	cl, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}

	var b bytes.Buffer
	writer := bufio.NewWriter(&b)
	reader := bufio.NewReader(&b)

	if err := types.WriteAll(cl, writer, table, []arrow.Record{record}); err != nil {
		t.Fatal(err)
	}
	writer.Flush()

	rawBytes, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	snap := cupaloy.New(
		cupaloy.SnapshotFileExtension(".jsonl"),
		cupaloy.SnapshotSubdirectory("testdata"),
	)
	snap.SnapshotT(t, string(rawBytes))

	byteReader := bytes.NewReader(rawBytes)

	ch := make(chan arrow.Record)
	var readErr error
	go func() {
		readErr = cl.Read(byteReader, table, ch)
		close(ch)
	}()
	received := make([]arrow.Record, 0, 2)
	for got := range ch {
		received = append(received, got)
	}
	require.Empty(t, plugin.RecordsDiff(table.ToArrowSchema(), []arrow.Record{record}, received))
	require.NoError(t, readErr)
	require.Equalf(t, 2, len(received), "got %d row(s), want %d", len(received), 2)
}

func BenchmarkWrite(b *testing.B) {
	table := schema.TestTable("test", schema.TestSourceOptions{})
	sourceName := "test-source"
	syncTime := time.Now().UTC().Round(time.Second)
	opts := schema.GenTestDataOptions{
		SourceName: sourceName,
		SyncTime:   syncTime,
		MaxRows:    1000,
	}
	tg := schema.NewTestDataGenerator(0)
	record := tg.Generate(table, opts)

	cl, err := NewClient()
	if err != nil {
		b.Fatal(err)
	}
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := types.WriteAll(cl, writer, table, []arrow.Record{record}); err != nil {
			b.Fatal(err)
		}
		err = writer.Flush()
		if err != nil {
			b.Fatal(err)
		}
		buf.Reset()
	}
}
