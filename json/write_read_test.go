package json

import (
	"bufio"
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/bradleyjkemp/cupaloy/v2"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/google/uuid"
)

func TestWriteRead(t *testing.T) {
	table := schema.TestTable("test", schema.TestSourceOptions{})
	sourceName := "test-source"
	syncTime := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	opts := schema.GenTestDataOptions{
		SourceName: sourceName,
		SyncTime:   syncTime,
		MaxRows:    2,
		StableUUID: uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		StableTime: time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC),
	}
	tg := schema.NewTestDataGenerator()
	records := tg.Generate(table, opts)

	cl, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}

	var b bytes.Buffer
	writer := bufio.NewWriter(&b)
	reader := bufio.NewReader(&b)

	if err := types.WriteAll(cl, writer, table, records); err != nil {
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
	totalCount := 0
	for got := range ch {
		if diff := plugin.RecordDiff(records[totalCount], got); diff != "" {
			t.Fatalf("got diff: %s", diff)
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
		MaxRows:    1000,
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
	for i := 0; i < b.N; i++ {
		if err := types.WriteAll(cl, writer, table, records); err != nil {
			b.Fatal(err)
		}
		err = writer.Flush()
		if err != nil {
			b.Fatal(err)
		}
		buf.Reset()
	}
}
