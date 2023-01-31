package parquet

import (
	"bytes"
	"fmt"
	"io"
	"reflect"

	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/segmentio/parquet-go"
)

func (*Client) Read(f io.Reader, table *schema.Table, sourceName string, res chan<- []any) error {
	sourceNameIndex := table.Columns.Index(schema.CqSourceNameColumn.Name)
	if sourceNameIndex == -1 {
		return fmt.Errorf("could not find column %s in table %s", schema.CqSourceNameColumn.Name, table.Name)
	}

	aStruct := makeStruct(table.Columns)
	s := parquet.SchemaOf(aStruct)

	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, f); err != nil {
		return err
	}
	seekableReader := bytes.NewReader(buf.Bytes())

	// TODO use sourceNameIndex
	_ = sourceName

	r := parquet.NewReader(seekableReader, schemaSetter{Schema: s})
	for {
		row := reflect.New(reflect.TypeOf(aStruct)).Interface()
		if err := r.Read(row); err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		res <- structToArray(row)
	}
	return nil
}

func structToArray(s any) []any {
	v := reflect.ValueOf(s)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil
	}
	a := make([]any, v.NumField())
	for i := 0; i < v.NumField(); i++ {
		a[i] = v.Field(i).Interface()
	}
	return a
}
