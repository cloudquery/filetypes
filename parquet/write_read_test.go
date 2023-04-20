package parquet

import (
	"bufio"
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/cloudquery/plugin-sdk/v2/plugins/destination"
	"github.com/cloudquery/plugin-sdk/v2/testdata"
)

func TestWriteRead(t *testing.T) {
	var b bytes.Buffer
	table := testdata.TestTable("test")
	arrowSchema := table.ToArrowSchema()
	sourceName := "test-source"
	syncTime := time.Now().UTC().Round(1 * time.Second)
	opts := testdata.GenTestDataOptions{
		SourceName: sourceName,
		SyncTime:   syncTime,
		MaxRows:    2,
	}
	records := testdata.GenTestData(arrowSchema, opts)
	for _, r := range records {
		r.Retain()
	}
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()
	writer := bufio.NewWriter(&b)
	reader := bufio.NewReader(&b)

	cl, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.WriteTableBatch(writer, arrowSchema, records); err != nil {
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
		readErr = cl.Read(byteReader, arrowSchema, "test-source", ch)
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
	if totalCount != 2 {
		t.Fatalf("expected 2 rows, got %d", totalCount)
	}
}
