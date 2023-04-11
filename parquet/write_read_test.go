package parquet

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/compute"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/cloudquery/plugin-sdk/plugins/destination"
	"github.com/cloudquery/plugin-sdk/testdata"
	"github.com/cloudquery/plugin-sdk/types"
)

func TestWriteRead(t *testing.T) {
	var b bytes.Buffer
	table := testdata.TestTable("test")
	sch := table.ToArrowSchema()
	sourceName := "test-source"
	syncTime := time.Now().UTC().Round(1 * time.Second)
	// TODO: use checked allocator here; can't right now because there
	//       are memory leaks in the arrow parquet reader implementation :(
	// mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	// defer mem.AssertSize(t, 0)
	mem := memory.NewGoAllocator()
	opts := testdata.GenTestDataOptions{
		SourceName: sourceName,
		SyncTime:   syncTime,
		MaxRows:    1,
	}
	records := testdata.GenTestData(mem, sch, opts)
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
	if err := cl.WriteTableBatch(writer, table, records); err != nil {
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
		readErr = cl.Read(byteReader, table, "test-source", ch)
		close(ch)
	}()
	totalCount := 0
	for got := range ch {
		baseRecord, err := castExtensionColsToStorageType(mem, records[totalCount])
		if err != nil {
			t.Fatalf("failed to cast extensions to storage type for comparison: %v", err)
		}
		if diff := destination.RecordDiff(baseRecord, got); diff != "" {
			t.Fatalf("got diff: %s", diff)
		}
		totalCount++
	}
	if readErr != nil {
		t.Fatal(readErr)
	}
	if totalCount != 1 {
		t.Fatalf("expected 1 row, got %d", totalCount)
	}
}

func castExtensionColsToStorageType(mem memory.Allocator, rec arrow.Record) (arrow.Record, error) {
	oldFields := rec.Schema().Fields()
	fields := make([]arrow.Field, len(oldFields))
	copy(fields, oldFields)
	for i, f := range fields {
		switch {
		case f.Type.ID() == arrow.EXTENSION:
			fields[i].Type = f.Type.(arrow.ExtensionType).StorageType()
		case arrow.TypeEqual(f.Type, arrow.ListOf(types.NewUUIDType())),
			arrow.TypeEqual(f.Type, arrow.ListOf(types.NewInetType())),
			arrow.TypeEqual(f.Type, arrow.ListOf(types.NewJSONType())),
			arrow.TypeEqual(f.Type, arrow.ListOf(types.NewMacType())):
			fields[i].Type = arrow.ListOf(f.Type.(*arrow.ListType).Elem().(arrow.ExtensionType).StorageType())
		}
	}

	md := rec.Schema().Metadata()
	newSchema := arrow.NewSchema(fields, &md)
	ctx := context.Background()
	rb := array.NewRecordBuilder(mem, newSchema)

	defer rb.Release()
	for c := 0; c < int(rec.NumCols()); c++ {
		col := rec.Column(c)
		if col.DataType().ID() == arrow.EXTENSION {
			storageType := col.DataType().(arrow.ExtensionType).StorageType()
			arr, err := compute.CastToType(ctx, rec.Column(c), storageType)
			if err != nil {
				return nil, fmt.Errorf("failed to cast col %v to %v: %w", rec.ColumnName(c), storageType, err)
			}
			b, err := arr.MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
			}
			err = rb.Field(c).UnmarshalJSON(b)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
			}
		} else if arrow.TypeEqual(col.DataType(), arrow.ListOf(types.NewUUIDType())) {
			castListOf(ctx, rec, c, rb, types.NewUUIDType().StorageType())
		} else if arrow.TypeEqual(col.DataType(), arrow.ListOf(types.NewJSONType())) {
			castListOf(ctx, rec, c, rb, types.NewJSONType().StorageType())
		} else if arrow.TypeEqual(col.DataType(), arrow.ListOf(types.NewInetType())) {
			castListOf(ctx, rec, c, rb, types.NewInetType().StorageType())
		} else if arrow.TypeEqual(col.DataType(), arrow.ListOf(types.NewMacType())) {
			castListOf(ctx, rec, c, rb, types.NewMacType().StorageType())
		} else {
			b, err := rec.Column(c).MarshalJSON()
			if err != nil {
				return nil, fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
			}
			err = rb.Field(c).UnmarshalJSON(b)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
			}
		}
	}
	return rb.NewRecord(), nil
}

func castListOf(ctx context.Context, rec arrow.Record, c int, rb *array.RecordBuilder, storageType arrow.DataType) error {
	arr, err := compute.CastToType(ctx, rec.Column(c), arrow.ListOf(storageType))
	if err != nil {
		return fmt.Errorf("failed to cast col %v to %v: %w", rec.ColumnName(c), storageType, err)
	}
	b, err := arr.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal col %v: %w", rec.ColumnName(c), err)
	}
	err = rb.Field(c).UnmarshalJSON(b)
	if err != nil {
		return fmt.Errorf("failed to unmarshal col %v: %w", rec.ColumnName(c), err)
	}
	return nil
}
