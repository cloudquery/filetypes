package parquet

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/cloudquery/plugin-sdk/v4/types"
	"github.com/stretchr/testify/require"
)

const (
	parquetString = "optional byte_array (String)"
	parquetJSON   = parquetString
	parquetBool   = "optional boolean"
)

func cloudflareCertificatePacksV11() *schema.Table {
	return &schema.Table{Name: "cloudflare_certificate_packs", Columns: schema.ColumnList{
		schema.CqIDColumn,
		schema.CqParentIDColumn,
		{Name: "account_id", Type: arrow.BinaryTypes.String},
		{Name: "zone_id", Type: arrow.BinaryTypes.String},
		{Name: "id", Type: arrow.BinaryTypes.String, PrimaryKey: true},
		{Name: "primary_certificate", Type: arrow.BinaryTypes.String},
		{Name: "certificate_authority", Type: arrow.BinaryTypes.String},
		{Name: "type", Type: arrow.BinaryTypes.String},
		{Name: "hosts", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		{Name: "status", Type: arrow.BinaryTypes.String},
		{Name: "certificates", Type: types.ExtensionTypes.JSON},
		{Name: "created_on", Type: arrow.BinaryTypes.String},
		{Name: "validity_days", Type: arrow.PrimitiveTypes.Float64},
		{Name: "validation_method", Type: arrow.BinaryTypes.String},
	}}
}

func cloudflareCertificatePacksV12() *schema.Table {
	return &schema.Table{Name: "cloudflare_certificate_packs", Columns: schema.ColumnList{
		schema.CqIDColumn,
		schema.CqParentIDColumn,
		{Name: "account_id", Type: arrow.BinaryTypes.String},
		{Name: "zone_id", Type: arrow.BinaryTypes.String},
		{Name: "id", Type: arrow.BinaryTypes.String, PrimaryKey: true},
		{Name: "validity_days", Type: arrow.PrimitiveTypes.Float64},
		{Name: "certificates", Type: types.ExtensionTypes.JSON},
		{Name: "hosts", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		{Name: "status", Type: arrow.BinaryTypes.String},
		{Name: "type", Type: arrow.BinaryTypes.String},
		{Name: "certificate_authority", Type: arrow.BinaryTypes.String},
		{Name: "cloudflare_branding", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "primary_certificate", Type: arrow.BinaryTypes.String},
		{Name: "validation_errors", Type: types.ExtensionTypes.JSON},
		{Name: "validation_method", Type: arrow.BinaryTypes.String},
		{Name: "validation_records", Type: types.ExtensionTypes.JSON},
	}}
}

func TestParquetSchemaMatchesWrittenFile(t *testing.T) {
	table := schema.TestTable("test", schema.TestSourceOptions{})
	cl, err := NewClient()
	require.NoError(t, err)

	var b bytes.Buffer
	h, err := cl.WriteHeader(&b, table)
	require.NoError(t, err)
	require.NoError(t, h.WriteFooter())

	reader, err := file.NewParquetReader(bytes.NewReader(b.Bytes()))
	require.NoError(t, err)
	defer reader.Close()

	generated, err := cl.ParquetSchema(table)
	require.NoError(t, err)
	require.True(t, reader.MetaData().Schema.Equals(generated))
}

func TestAssessTable(t *testing.T) {
	stringList := arrow.ListOf(arrow.BinaryTypes.String)
	tagsTable := func(dataType arrow.DataType) *schema.Table {
		return &schema.Table{Name: "datadog_monitors", Columns: schema.ColumnList{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, PrimaryKey: true, NotNull: true},
			{Name: "tags", Type: dataType},
		}}
	}
	tests := []struct {
		name string
		pair plugin.TablePair
		want plugin.TableFinding
	}{
		{
			name: "cloudflare certificate packs v11.4.0 to v12.0.0",
			pair: plugin.TablePair{Old: cloudflareCertificatePacksV11(), New: cloudflareCertificatePacksV12()},
			want: plugin.TableFinding{
				TableName: "cloudflare_certificate_packs",
				Category:  plugin.AssessCategoryFileSchemaChanged,
				Columns: []plugin.ColumnFinding{
					{ColumnName: "created_on", Category: plugin.AssessCategoryFileSchemaChanged, OldType: parquetString},
					{ColumnName: "cloudflare_branding", Category: plugin.AssessCategoryFileSchemaChanged, NewType: parquetBool},
					{ColumnName: "validation_errors", Category: plugin.AssessCategoryFileSchemaChanged, NewType: parquetJSON},
					{ColumnName: "validation_records", Category: plugin.AssessCategoryFileSchemaChanged, NewType: parquetJSON},
				},
			},
		},
		{
			name: "list of strings to json",
			pair: plugin.TablePair{Old: tagsTable(stringList), New: tagsTable(types.ExtensionTypes.JSON)},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryFileSchemaChanged,
				Columns: []plugin.ColumnFinding{{
					ColumnName: "tags",
					Category:   plugin.AssessCategoryFileSchemaChanged,
					OldType:    "optional group (List) {list: repeated group {element: optional byte_array (String)}}",
					NewType:    parquetJSON,
				}},
			},
		},
		{
			name: "types that write the same parquet type",
			pair: plugin.TablePair{Old: tagsTable(arrow.BinaryTypes.LargeString), New: tagsTable(types.ExtensionTypes.UUID)},
			want: plugin.TableFinding{TableName: "datadog_monitors", Category: plugin.AssessCategoryNoChange},
		},
		{
			name: "timestamp precision change",
			pair: plugin.TablePair{Old: tagsTable(arrow.FixedWidthTypes.Timestamp_ms), New: tagsTable(arrow.FixedWidthTypes.Timestamp_us)},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryFileSchemaChanged,
				Columns: []plugin.ColumnFinding{{
					ColumnName: "tags",
					Category:   plugin.AssessCategoryFileSchemaChanged,
					OldType:    "optional int64 (Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds, is_from_converted_type=false, force_set_converted_type=true))",
					NewType:    "optional int64 (Timestamp(isAdjustedToUTC=true, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=true))",
				}},
			},
		},
		{
			name: "same table",
			pair: plugin.TablePair{Old: cloudflareCertificatePacksV12(), New: cloudflareCertificatePacksV12()},
			want: plugin.TableFinding{TableName: "cloudflare_certificate_packs", Category: plugin.AssessCategoryNoChange},
		},
		{
			name: "added table",
			pair: plugin.TablePair{New: tagsTable(stringList)},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryFileSchemaChanged,
				Columns: []plugin.ColumnFinding{
					{ColumnName: "id", Category: plugin.AssessCategoryFileSchemaChanged, NewType: "required int64 (Int(bitWidth=64, isSigned=true))"},
					{ColumnName: "tags", Category: plugin.AssessCategoryFileSchemaChanged, NewType: "optional group (List) {list: repeated group {element: optional byte_array (String)}}"},
				},
			},
		},
		{
			name: "removed table",
			pair: plugin.TablePair{Old: tagsTable(stringList)},
			want: plugin.TableFinding{TableName: "datadog_monitors", Category: plugin.AssessCategoryTableRemoved},
		},
	}
	cl, err := NewClient()
	require.NoError(t, err)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cl.AssessTable(tc.pair)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
