package filetypes_test

import (
	"bufio"
	fmt "fmt"
	"io"
	"testing"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/cloudquery/filetypes/v4"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/stretchr/testify/assert"
)

type uploadHelper struct {
	t         *testing.T
	failAfter int
	expect    []byte
	expectAt  int
}

var errTest = fmt.Errorf("test error")

func (u *uploadHelper) Upload(r io.Reader) error {
	s := bufio.NewScanner(r)
	i := 0
	for s.Scan() {
		if u.failAfter > 0 && i == u.failAfter {
			return errTest
		}
		if u.expect != nil && i == u.expectAt {
			if !assert.Equal(u.t, u.expect, s.Bytes()) {
				return fmt.Errorf("assertion failed")
			}
		}
		i++
	}
	return s.Err()
}

func TestHappyPath(t *testing.T) {
	cl, err := filetypes.NewClient(&filetypes.FileSpec{
		Format: filetypes.FormatTypeJSON,
	})
	if !assert.NoError(t, err) {
		return
	}

	table := &schema.Table{
		Name: "test",
		Columns: []schema.Column{
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
	}

	u := &uploadHelper{
		t:        t,
		expect:   []byte(`{"name":"bar"}`),
		expectAt: 1,
	}
	s, err := cl.StartStream(table, u.Upload)
	if !assert.NoError(t, err) {
		return
	}

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, table.ToArrowSchema())
	bldr.Field(0).(*array.StringBuilder).Append("foo")
	bldr.Field(0).(*array.StringBuilder).Append("bar")
	record := bldr.NewRecord()

	if !assert.NoError(t, s.Write([]arrow.Record{record})) {
		return
	}

	assert.NoError(t, s.Finish())
}

func TestWriteError(t *testing.T) {
	cl, err := filetypes.NewClient(&filetypes.FileSpec{
		Format: filetypes.FormatTypeJSON,
	})
	if !assert.NoError(t, err) {
		return
	}

	table := &schema.Table{
		Name: "test",
		Columns: []schema.Column{
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
	}

	u := &uploadHelper{
		t:         t,
		failAfter: 1,
	}
	s, err := cl.StartStream(table, u.Upload)
	if !assert.NoError(t, err) {
		return
	}

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, table.ToArrowSchema())
	bldr.Field(0).(*array.StringBuilder).Append("foo")
	bldr.Field(0).(*array.StringBuilder).Append("bar")
	record := bldr.NewRecord()

	if !assert.NoError(t, s.Write([]arrow.Record{record})) {
		return
	}

	assert.ErrorIs(t, s.Finish(), errTest)
}

func TestCloseError(t *testing.T) {
	cl, err := filetypes.NewClient(&filetypes.FileSpec{
		Format: filetypes.FormatTypeJSON,
	})
	if !assert.NoError(t, err) {
		return
	}

	table := &schema.Table{
		Name: "test",
		Columns: []schema.Column{
			{Name: "name", Type: arrow.BinaryTypes.String},
		},
	}

	u := &uploadHelper{
		t: t,
	}
	s, err := cl.StartStream(table, u.Upload)
	if !assert.NoError(t, err) {
		return
	}

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, table.ToArrowSchema())
	bldr.Field(0).(*array.StringBuilder).Append("foo")
	bldr.Field(0).(*array.StringBuilder).Append("bar")
	record := bldr.NewRecord()

	if !assert.NoError(t, s.Write([]arrow.Record{record})) {
		return
	}

	assert.ErrorIs(t, s.FinishWithError(errTest), errTest)
}
