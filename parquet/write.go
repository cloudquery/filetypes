package parquet

import (
	"fmt"
	"io"
	"reflect"

	"github.com/cloudquery/plugin-sdk/schema"
	"github.com/segmentio/parquet-go"
)

func (c *Client) WriteTableBatch(w io.Writer, table *schema.Table, resources [][]any) error {
	aStruct := c.makeStruct(table.Columns)
	s := parquet.SchemaOf(aStruct)
	pw := parquet.NewWriter(w, schemaSetter{Schema: s})

	for i := range resources {
		obj := arrayToStruct(resources[i], aStruct)
		if err := pw.Write(obj); err != nil {
			return err
		}
	}

	return pw.Close()
}

func arrayToStruct(a []any, wantType any) any {
	t := reflect.TypeOf(wantType)
	s := reflect.New(t).Elem()
	if al, sl := len(a), t.NumField(); al != sl {
		panic(fmt.Sprintf("array length %d != struct length %d", al, sl))
	}

	for i := range a {
		val := reflect.ValueOf(a[i])
		if a[i] == nil {
			continue
		}

		n := formatFieldNameWithIndex(i)
		s.FieldByName(n).Set(val)
	}

	return s.Interface()
}
