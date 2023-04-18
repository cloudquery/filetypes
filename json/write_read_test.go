package json

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

func TestWrite(t *testing.T) {
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
	}
	records := testdata.GenTestData(mem, arrowSchema, opts)
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()
	cl, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.WriteTableBatch(&b, arrowSchema, records); err != nil {
		t.Fatal(err)
	}
	t.Log(b.String())
}

func TestWriteRead(t *testing.T) {
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
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()
	cl, err := NewClient()
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
		cupaloy.SnapshotFileExtension(".jsonl"),
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
