package parquet

import (
	"bufio"
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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
	tg := schema.NewTestDataGenerator(0)
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
	received, total := make([]arrow.Record, 0, rows), 0
	for got := range ch {
		received = append(received, got)
		total += int(got.NumRows())
	}
	require.Empty(t, plugin.RecordsDiff(table.ToArrowSchema(), []arrow.Record{record}, received))
	require.NoError(t, readErr)
	require.Equalf(t, rows, total, "got %d row(s), want %d", total, rows)
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
		NullRows:   true,
	}
	tg := schema.NewTestDataGenerator(0)
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
	received, total := make([]arrow.Record, 0, rows), 0
	for got := range ch {
		received = append(received, got)
		total += int(got.NumRows())
	}
	require.Empty(t, plugin.RecordsDiff(table.ToArrowSchema(), []arrow.Record{record}, received))
	require.NoError(t, readErr)
	require.Equalf(t, rows, total, "got %d row(s), want %d", total, rows)
}

func TestWrite_EmptySourceTablesBug(t *testing.T) {
	r := require.New(t)
	type syncSummary struct {
		SourceName   string   `json:"source_name"`
		SourceTables []string `json:"source_tables"`
	}
	summary := syncSummary{
		SourceName:   "test_source",
		SourceTables: []string{"test_table"},
	}
	md := arrow.NewMetadata(
		[]string{schema.MetadataTableName},
		[]string{"cloudquery_sync_summaries"},
	)
	sch := arrow.NewSchema([]arrow.Field{
		{Name: "source_name", Type: arrow.BinaryTypes.String},
		{Name: "source_tables", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	}, &md)
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, sch)
	defer bldr.Release()
	r.NoError(bldr.Field(0).AppendValueFromString("test_source"))
	listBldr := bldr.Field(1).(*array.ListBuilder)
	strBldr := listBldr.ValueBuilder().(*array.StringBuilder)
	listBldr.Append(true)
	for _, table := range summary.SourceTables {
		strBldr.Append(table)
	}
	rec := bldr.NewRecord()
	defer rec.Release()

	tbl, err := schema.NewTableFromArrowSchema(sch)
	r.NoError(err)

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	cl, err := NewClient()
	r.NoError(err)
	r.NoError(types.WriteAll(cl, writer, tbl, []arrow.Record{rec}))
	r.NoError(writer.Flush())

	reader := bufio.NewReader(&buf)
	rawBytes, err := io.ReadAll(reader)
	r.NoError(err)
	byteReader := bytes.NewReader(rawBytes)
	ch := make(chan arrow.Record)
	var readErr error
	go func() {
		readErr = cl.Read(byteReader, tbl, ch)
		close(ch)
	}()
	records := make([]arrow.Record, 0)
	for rec := range ch {
		records = append(records, rec)
	}
	r.NoError(readErr)
	r.Len(records, 1)
	readRec := records[0]
	defer readRec.Release()
	r.Equal(int64(1), readRec.NumRows())
	r.Equal("test_source", readRec.Column(0).(*array.String).Value(0))
	listArray := readRec.Column(1).(*array.List)
	strArray := listArray.ListValues().(*array.String)
	r.Equal(1, listArray.Len())
	r.Equal("test_table", strArray.Value(0))
}

func slice(r arrow.Record) []arrow.Record {
	res := make([]arrow.Record, r.NumRows())
	for i := int64(0); i < r.NumRows(); i++ {
		res[i] = r.NewSlice(i, i+1)
	}
	return res
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
	tg := schema.NewTestDataGenerator(0)
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
