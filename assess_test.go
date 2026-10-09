package filetypes_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/cloudquery/filetypes/v4"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/cloudquery/plugin-sdk/v4/types"
	"github.com/stretchr/testify/require"
)

func table(columns ...schema.Column) *schema.Table {
	return &schema.Table{Name: "datadog_monitors", Columns: columns}
}

var (
	idColumn         = schema.Column{Name: "id", Type: arrow.PrimitiveTypes.Int64}
	tagsStringList   = schema.Column{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)}
	tagsJSON         = schema.Column{Name: "tags", Type: types.ExtensionTypes.JSON}
	nameString       = schema.Column{Name: "name", Type: arrow.BinaryTypes.String}
	nameJSON         = schema.Column{Name: "name", Type: types.ExtensionTypes.JSON}
	countInt         = schema.Column{Name: "count", Type: arrow.PrimitiveTypes.Int64}
	countString      = schema.Column{Name: "count", Type: arrow.BinaryTypes.String}
	escapingValue    = `"comma, \"quote\", back\\slash\nnew line\ttab é"`
	jsonSpec         = &filetypes.FileSpec{Format: filetypes.FormatTypeJSON}
	csvSpec          = &filetypes.FileSpec{Format: filetypes.FormatTypeCSV}
	parquetSpec      = &filetypes.FileSpec{Format: filetypes.FormatTypeParquet}
	csvNoHeaderSpec  = &filetypes.FileSpec{Format: filetypes.FormatTypeCSV, FormatSpec: map[string]any{"skip_header": true, "delimiter": ";"}}
	datadogTagsPair  = plugin.TablePair{Old: table(idColumn, tagsStringList), New: table(idColumn, tagsJSON)}
	datadogTagsTypes = plugin.ColumnFinding{ColumnName: "tags", OldType: "list<item: utf8, nullable>", NewType: "json"}
)

func notNull(column schema.Column) schema.Column {
	column.NotNull = true
	return column
}

func columnFinding(base plugin.ColumnFinding, category plugin.AssessCategory, evidence ...plugin.Evidence) plugin.ColumnFinding {
	base.Category = category
	base.Evidence = evidence
	return base
}

func TestAssessTable(t *testing.T) {
	tests := []struct {
		name string
		spec *filetypes.FileSpec
		pair plugin.TablePair
		want plugin.TableFinding
	}{
		{
			name: "json list of strings to json keeps output",
			spec: jsonSpec,
			pair: datadogTagsPair,
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryNoChange,
				Columns: []plugin.ColumnFinding{columnFinding(datadogTagsTypes, plugin.AssessCategoryNoChange,
					plugin.Evidence{SyntheticValue: `["env:prod"]`, Before: `{"tags":["env:prod"]}`, After: `{"tags":["env:prod"]}`},
					plugin.Evidence{SyntheticValue: `null`, Before: `{"tags":null}`, After: `{"tags":null}`},
					plugin.Evidence{SyntheticValue: `[]`, Before: `{"tags":[]}`, After: `{"tags":[]}`},
					plugin.Evidence{SyntheticValue: `[` + escapingValue + `]`, Before: `{"tags":[` + escapingValue + `]}`, After: `{"tags":[` + escapingValue + `]}`},
				)},
			},
		},
		{
			name: "csv list of strings to json keeps output",
			spec: csvSpec,
			pair: datadogTagsPair,
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryNoChange,
				Columns: []plugin.ColumnFinding{columnFinding(datadogTagsTypes, plugin.AssessCategoryNoChange,
					plugin.Evidence{SyntheticValue: `["env:prod"]`, Before: "tags\n\"[\"\"env:prod\"\"]\"", After: "tags\n\"[\"\"env:prod\"\"]\""},
					plugin.Evidence{SyntheticValue: `null`, Before: "tags\n", After: "tags\n"},
					plugin.Evidence{SyntheticValue: `[]`, Before: "tags\n[]", After: "tags\n[]"},
					plugin.Evidence{
						SyntheticValue: `[` + escapingValue + `]`,
						Before:         "tags\n\"[\"\"comma, \\\"\"quote\\\"\", back\\\\slash\\nnew line\\ttab é\"\"]\"",
						After:          "tags\n\"[\"\"comma, \\\"\"quote\\\"\", back\\\\slash\\nnew line\\ttab é\"\"]\"",
					},
				)},
			},
		},
		{
			name: "json string to json keeps output",
			spec: jsonSpec,
			pair: plugin.TablePair{Old: table(nameString), New: table(nameJSON)},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryNoChange,
				Columns: []plugin.ColumnFinding{{
					ColumnName: "name", Category: plugin.AssessCategoryNoChange, OldType: "utf8", NewType: "json",
					Evidence: []plugin.Evidence{
						{SyntheticValue: `"env:prod"`, Before: `{"name":"env:prod"}`, After: `{"name":"env:prod"}`},
						{SyntheticValue: `null`, Before: `{"name":null}`, After: `{"name":null}`},
						{SyntheticValue: escapingValue, Before: `{"name":` + escapingValue + `}`, After: `{"name":` + escapingValue + `}`},
					},
				}},
			},
		},
		{
			name: "csv string to json changes quoting",
			spec: csvSpec,
			pair: plugin.TablePair{Old: table(nameString), New: table(nameJSON)},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryFileSchemaChanged,
				Columns: []plugin.ColumnFinding{{
					ColumnName: "name", Category: plugin.AssessCategoryFileSchemaChanged, OldType: "utf8", NewType: "json",
					Evidence: []plugin.Evidence{
						{SyntheticValue: `"env:prod"`, Before: "name\nenv:prod", After: "name\n\"\"\"env:prod\"\"\""},
						{SyntheticValue: `null`, Before: "name\n", After: "name\n"},
						{
							SyntheticValue: escapingValue,
							Before:         "name\n\"comma, \"\"quote\"\", back\\slash\nnew line\ttab é\"",
							After:          "name\n\"\"\"comma, \\\"\"quote\\\"\", back\\\\slash\\nnew line\\ttab é\"\"\"",
						},
					},
				}},
			},
		},
		{
			name: "csv reordered columns change header",
			spec: csvSpec,
			pair: plugin.TablePair{Old: table(idColumn, nameString), New: table(nameString, idColumn)},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryFileSchemaChanged,
				Evidence:  []plugin.Evidence{{Before: "id,name\n42,env:prod", After: "name,id\nenv:prod,42"}},
			},
		},
		{
			name: "csv options apply to evidence",
			spec: csvNoHeaderSpec,
			pair: plugin.TablePair{Old: table(idColumn, nameString), New: table(nameString, idColumn)},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryFileSchemaChanged,
				Evidence:  []plugin.Evidence{{Before: "42;env:prod", After: "env:prod;42"}},
			},
		},
		{
			name: "json reordered columns keep output",
			spec: jsonSpec,
			pair: plugin.TablePair{Old: table(idColumn, nameString), New: table(nameString, idColumn)},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryNoChange,
				Evidence:  []plugin.Evidence{{Before: `{"id":42,"name":"env:prod"}`, After: `{"id":42,"name":"env:prod"}`}},
			},
		},
		{
			name: "json added and removed columns",
			spec: jsonSpec,
			pair: plugin.TablePair{Old: table(idColumn, nameString), New: table(idColumn, tagsJSON)},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryFileSchemaChanged,
				Columns: []plugin.ColumnFinding{
					{ColumnName: "name", Category: plugin.AssessCategoryFileSchemaChanged, OldType: "utf8"},
					{ColumnName: "tags", Category: plugin.AssessCategoryAutomaticallyMigratable, NewType: "json"},
				},
				Evidence: []plugin.Evidence{{Before: `{"id":42,"name":"env:prod"}`, After: `{"id":42,"tags":{"env":"prod"}}`}},
			},
		},
		{
			name: "no equivalent values is unknown",
			spec: jsonSpec,
			pair: plugin.TablePair{Old: table(idColumn, countInt), New: table(idColumn, countString)},
			want: plugin.TableFinding{
				TableName:                "datadog_monitors",
				Category:                 plugin.AssessCategoryUnknown,
				Columns:                  []plugin.ColumnFinding{{ColumnName: "count", Category: plugin.AssessCategoryUnknown, OldType: "int64", NewType: "utf8"}},
				IncompleteCoverageReason: "unable to compare: no equivalent values for columns count",
			},
		},
		{
			name: "changed output with unknown columns is incomplete",
			spec: csvSpec,
			pair: plugin.TablePair{Old: table(nameString, countInt), New: table(nameJSON, countString)},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryFileSchemaChanged,
				Columns: []plugin.ColumnFinding{
					{
						ColumnName: "name", Category: plugin.AssessCategoryFileSchemaChanged, OldType: "utf8", NewType: "json",
						Evidence: []plugin.Evidence{
							{SyntheticValue: `"env:prod"`, Before: "name\nenv:prod", After: "name\n\"\"\"env:prod\"\"\""},
							{SyntheticValue: `null`, Before: "name\n", After: "name\n"},
							{
								SyntheticValue: escapingValue,
								Before:         "name\n\"comma, \"\"quote\"\", back\\slash\nnew line\ttab é\"",
								After:          "name\n\"\"\"comma, \\\"\"quote\\\"\", back\\\\slash\\nnew line\\ttab é\"\"\"",
							},
						},
					},
					{ColumnName: "count", Category: plugin.AssessCategoryUnknown, OldType: "int64", NewType: "utf8"},
				},
				IncompleteCoverageReason: "unable to compare: no equivalent values for columns count",
			},
		},
		{
			name: "unchanged table",
			spec: csvSpec,
			pair: plugin.TablePair{Old: table(idColumn, tagsJSON), New: table(idColumn, tagsJSON)},
			want: plugin.TableFinding{TableName: "datadog_monitors", Category: plugin.AssessCategoryNoChange},
		},
		{
			name: "removed table",
			spec: jsonSpec,
			pair: plugin.TablePair{Old: table(idColumn)},
			want: plugin.TableFinding{TableName: "datadog_monitors", Category: plugin.AssessCategoryTableRemoved},
		},
		{
			name: "added table",
			spec: csvSpec,
			pair: plugin.TablePair{New: table(idColumn)},
			want: plugin.TableFinding{
				TableName:          "datadog_monitors",
				Category:           plugin.AssessCategoryAutomaticallyMigratable,
				SafeModeBehavior:   "new files add the new table",
				ForcedModeBehavior: "new files add the new table",
				Columns:            []plugin.ColumnFinding{{ColumnName: "id", Category: plugin.AssessCategoryAutomaticallyMigratable, NewType: "int64"}},
			},
		},
		{
			name: "json added nullable column extends output",
			spec: jsonSpec,
			pair: plugin.TablePair{Old: table(idColumn, nameString), New: table(tagsJSON, idColumn, nameString)},
			want: plugin.TableFinding{
				TableName:          "datadog_monitors",
				Category:           plugin.AssessCategoryAutomaticallyMigratable,
				SafeModeBehavior:   "new files add the new columns, existing files are not changed",
				ForcedModeBehavior: "new files add the new columns, existing files are not changed",
				Columns:            []plugin.ColumnFinding{{ColumnName: "tags", Category: plugin.AssessCategoryAutomaticallyMigratable, NewType: "json"}},
				Evidence:           []plugin.Evidence{{Before: `{"id":42,"name":"env:prod"}`, After: `{"id":42,"name":"env:prod","tags":{"env":"prod"}}`}},
			},
		},
		{
			name: "json added not null column changes output",
			spec: jsonSpec,
			pair: plugin.TablePair{Old: table(idColumn), New: table(idColumn, notNull(nameString))},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryFileSchemaChanged,
				Columns:   []plugin.ColumnFinding{{ColumnName: "name", Category: plugin.AssessCategoryFileSchemaChanged, NewType: "utf8"}},
				Evidence:  []plugin.Evidence{{Before: `{"id":42}`, After: `{"id":42,"name":"env:prod"}`}},
			},
		},
		{
			name: "csv column appended with header extends output",
			spec: csvSpec,
			pair: plugin.TablePair{Old: table(idColumn), New: table(idColumn, nameString)},
			want: plugin.TableFinding{
				TableName:          "datadog_monitors",
				Category:           plugin.AssessCategoryAutomaticallyMigratable,
				SafeModeBehavior:   "new files add the new columns, existing files are not changed",
				ForcedModeBehavior: "new files add the new columns, existing files are not changed",
				Columns:            []plugin.ColumnFinding{{ColumnName: "name", Category: plugin.AssessCategoryAutomaticallyMigratable, NewType: "utf8"}},
				Evidence:           []plugin.Evidence{{Before: "id\n42", After: "id,name\n42,env:prod"}},
			},
		},
		{
			name: "csv column appended without header changes output",
			spec: csvNoHeaderSpec,
			pair: plugin.TablePair{Old: table(idColumn), New: table(idColumn, nameString)},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryFileSchemaChanged,
				Columns:   []plugin.ColumnFinding{{ColumnName: "name", Category: plugin.AssessCategoryFileSchemaChanged, NewType: "utf8"}},
				Evidence:  []plugin.Evidence{{Before: "42", After: "42;env:prod"}},
			},
		},
		{
			name: "csv column inserted before existing columns changes output",
			spec: csvSpec,
			pair: plugin.TablePair{Old: table(idColumn), New: table(nameString, idColumn)},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryFileSchemaChanged,
				Columns:   []plugin.ColumnFinding{{ColumnName: "name", Category: plugin.AssessCategoryFileSchemaChanged, NewType: "utf8"}},
				Evidence:  []plugin.Evidence{{Before: "id\n42", After: "name,id\nenv:prod,42"}},
			},
		},
		{
			name: "parquet added nullable columns extend schema",
			spec: parquetSpec,
			pair: plugin.TablePair{Old: table(notNull(idColumn)), New: table(nameString, notNull(idColumn), tagsStringList)},
			want: plugin.TableFinding{
				TableName:          "datadog_monitors",
				Category:           plugin.AssessCategoryAutomaticallyMigratable,
				SafeModeBehavior:   "new files add the new columns, existing files are not changed",
				ForcedModeBehavior: "new files add the new columns, existing files are not changed",
				Columns: []plugin.ColumnFinding{
					{ColumnName: "name", Category: plugin.AssessCategoryAutomaticallyMigratable, NewType: "optional byte_array (String)"},
					{ColumnName: "tags", Category: plugin.AssessCategoryAutomaticallyMigratable, NewType: "optional group (List) {list: repeated group {element: optional byte_array (String)}}"},
				},
			},
		},
		{
			name: "parquet added required column changes schema",
			spec: parquetSpec,
			pair: plugin.TablePair{Old: table(nameString), New: table(nameString, notNull(idColumn))},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryFileSchemaChanged,
				Columns:   []plugin.ColumnFinding{{ColumnName: "id", Category: plugin.AssessCategoryFileSchemaChanged, NewType: "required int64 (Int(bitWidth=64, isSigned=true))"}},
			},
		},
		{
			name: "parquet optional to required column changes schema",
			spec: parquetSpec,
			pair: plugin.TablePair{Old: table(idColumn), New: table(notNull(idColumn), nameString)},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryFileSchemaChanged,
				Columns: []plugin.ColumnFinding{
					{ColumnName: "id", Category: plugin.AssessCategoryFileSchemaChanged, OldType: "optional int64 (Int(bitWidth=64, isSigned=true))", NewType: "required int64 (Int(bitWidth=64, isSigned=true))"},
					{ColumnName: "name", Category: plugin.AssessCategoryAutomaticallyMigratable, NewType: "optional byte_array (String)"},
				},
			},
		},
		{
			name: "parquet added table",
			spec: parquetSpec,
			pair: plugin.TablePair{New: table(notNull(idColumn))},
			want: plugin.TableFinding{
				TableName:          "datadog_monitors",
				Category:           plugin.AssessCategoryAutomaticallyMigratable,
				SafeModeBehavior:   "new files add the new table",
				ForcedModeBehavior: "new files add the new table",
				Columns:            []plugin.ColumnFinding{{ColumnName: "id", Category: plugin.AssessCategoryAutomaticallyMigratable, NewType: "required int64 (Int(bitWidth=64, isSigned=true))"}},
			},
		},
		{
			name: "parquet compares generated schemas",
			spec: parquetSpec,
			pair: plugin.TablePair{Old: table(idColumn, nameString), New: table(idColumn)},
			want: plugin.TableFinding{
				TableName: "datadog_monitors",
				Category:  plugin.AssessCategoryFileSchemaChanged,
				Columns:   []plugin.ColumnFinding{{ColumnName: "name", Category: plugin.AssessCategoryFileSchemaChanged, OldType: "optional byte_array (String)"}},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := *tc.spec
			cl, err := filetypes.NewClient(&spec)
			require.NoError(t, err)
			got, err := cl.AssessTable(tc.pair)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
