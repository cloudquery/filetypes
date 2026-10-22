package parquet

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
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
	if err := types.WriteAll(cl, writer, table, []arrow.RecordBatch{record}); err != nil {
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
	ch := make(chan arrow.RecordBatch)
	var readErr error
	go func() {
		readErr = cl.Read(byteReader, table, ch)
		close(ch)
	}()
	received, total := make([]arrow.RecordBatch, 0, rows), 0
	for got := range ch {
		received = append(received, got)
		total += int(got.NumRows())
	}
	require.Empty(t, plugin.RecordsDiff(table.ToArrowSchema(), []arrow.RecordBatch{record}, received))
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
	ch := make(chan arrow.RecordBatch)
	var readErr error
	go func() {
		readErr = cl.Read(byteReader, table, ch)
		close(ch)
	}()
	received, total := make([]arrow.RecordBatch, 0, rows), 0
	for got := range ch {
		received = append(received, got)
		total += int(got.NumRows())
	}
	require.Empty(t, plugin.RecordsDiff(table.ToArrowSchema(), []arrow.RecordBatch{record}, received))
	require.NoError(t, readErr)
	require.Equalf(t, rows, total, "got %d row(s), want %d", total, rows)
}

func slice(r arrow.RecordBatch) []arrow.RecordBatch {
	res := make([]arrow.RecordBatch, r.NumRows())
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

	if err := types.WriteAll(cl, writer, table, []arrow.RecordBatch{record}); err != nil {
		b.Fatal(err)
	}
	err = writer.Flush()
	if err != nil {
		b.Fatal(err)
	}
}

func TestWriteMultipleRowGroups(t *testing.T) {
	const rows = 20
	const maxRowGroupLength = 5

	table := schema.TestTable("test", schema.TestSourceOptions{})
	tg := schema.NewTestDataGenerator(0)
	record := tg.Generate(table, schema.GenTestDataOptions{
		SourceName: "test-source",
		SyncTime:   time.Now().UTC().Round(time.Second),
		MaxRows:    rows,
	})

	maxRowGroupLengthValue := int64(maxRowGroupLength)
	cl, err := NewClient(WithSpec(ParquetSpec{MaxRowGroupLength: &maxRowGroupLengthValue}))
	require.NoError(t, err)

	var b bytes.Buffer
	writer := bufio.NewWriter(&b)
	require.NoError(t, types.WriteAll(cl, writer, table, []arrow.RecordBatch{record}))
	require.NoError(t, writer.Flush())

	pf, err := file.NewParquetReader(bytes.NewReader(b.Bytes()))
	require.NoError(t, err)
	defer pf.Close()

	require.Equal(t, rows/maxRowGroupLength, pf.NumRowGroups())
	require.Greater(t, pf.NumRowGroups(), 1)
	require.EqualValues(t, rows, pf.NumRows())
}

func TestWriteReadSlicedNestedNullableStructWithLists(t *testing.T) {
	const (
		rows       = 100
		subStructs = 8
	)

	subType := arrow.StructOf(
		arrow.Field{Name: "ids", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
		arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	)
	fields := make([]arrow.Field, subStructs)
	for i := range fields {
		fields[i] = arrow.Field{Name: fmt.Sprintf("sub_%d", i), Type: subType, Nullable: true}
	}
	outerType := arrow.StructOf(fields...)
	sc := arrow.NewSchema([]arrow.Field{{Name: "outer", Type: outerType, Nullable: true}}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer bldr.Release()
	outer := bldr.Field(0).(*array.StructBuilder)
	for row := 0; row < rows; row++ {
		outer.Append(true)
		for i := 0; i < subStructs; i++ {
			sub := outer.FieldBuilder(i).(*array.StructBuilder)
			if i != row%subStructs {
				sub.AppendNull()
				continue
			}
			sub.Append(true)
			ids := sub.FieldBuilder(0).(*array.ListBuilder)
			ids.Append(true)
			for j := 0; j <= row%5; j++ {
				ids.ValueBuilder().(*array.Int64Builder).Append(int64(row*10 + j))
			}
			sub.FieldBuilder(1).(*array.Int64Builder).Append(int64(row))
		}
	}
	record := bldr.NewRecordBatch()
	defer record.Release()

	table := &schema.Table{Name: "test", Columns: schema.ColumnList{{Name: "outer", Type: outerType}}}
	cl, err := NewClient()
	require.NoError(t, err)

	for _, splitAt := range []int64{1, 3, 50, 99} {
		t.Run(fmt.Sprintf("split_at_%d", splitAt), func(t *testing.T) {
			head, tail := record.NewSlice(0, splitAt), record.NewSlice(splitAt, rows)
			defer head.Release()
			defer tail.Release()

			var b bytes.Buffer
			writer := bufio.NewWriter(&b)
			require.NoError(t, types.WriteAll(cl, writer, table, []arrow.RecordBatch{head, tail}))
			require.NoError(t, writer.Flush())

			ch := make(chan arrow.RecordBatch)
			var readErr error
			go func() {
				readErr = cl.Read(bytes.NewReader(b.Bytes()), table, ch)
				close(ch)
			}()
			var received []arrow.RecordBatch
			for got := range ch {
				received = append(received, got)
			}
			require.NoError(t, readErr)
			require.Empty(t, plugin.RecordsDiff(sc, []arrow.RecordBatch{record}, received))

			row := 0
			for _, got := range received {
				gotOuter := got.Column(0).(*array.Struct)
				for r := 0; r < gotOuter.Len(); r++ {
					for i := 0; i < subStructs; i++ {
						require.Equalf(t, i == row%subStructs, gotOuter.Field(i).IsValid(r), "row %d, sub_%d validity", row, i)
					}
					row++
				}
			}
			require.Equal(t, rows, row)
		})
	}
}
