package filetypes

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	csvfile "github.com/cloudquery/filetypes/v4/csv"
	"github.com/cloudquery/filetypes/v4/parquet"
	"github.com/cloudquery/filetypes/v4/types"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

const (
	behaviorNewColumns = "new files add the new columns, existing files are not changed"
	behaviorNewTable   = "new files add the new table"
)

// AssessTable reports how the table change affects the files written with the configured format.
// Parquet compares generated schemas. JSON and CSV serialize equivalent synthetic values under both schemas.
func (cl *Client) AssessTable(pair plugin.TablePair) (plugin.TableFinding, error) {
	if pair.New == nil {
		return plugin.TableFinding{TableName: pair.TableName(), Category: plugin.AssessCategoryTableRemoved}, nil
	}
	finding, outputChanged, err := cl.compareTables(pair)
	if err != nil {
		return plugin.TableFinding{}, err
	}
	for i, column := range finding.Columns {
		if column.OldType == "" && cl.isAdditiveColumn(pair, column.ColumnName) {
			finding.Columns[i].Category = plugin.AssessCategoryAutomaticallyMigratable
		}
	}
	classifyTable(&finding, pair, outputChanged)
	return finding, nil
}

func (cl *Client) compareTables(pair plugin.TablePair) (plugin.TableFinding, bool, error) {
	if pq, ok := cl.filetype.(*parquet.Client); ok {
		finding, err := pq.AssessTable(pair)
		return finding, false, err
	}
	finding := plugin.TableFinding{TableName: pair.TableName()}
	oldTable := pair.Old
	if oldTable == nil {
		oldTable = &schema.Table{Name: pair.New.Name}
	}

	for _, oldColumn := range oldTable.Columns {
		newColumn := pair.New.Columns.Get(oldColumn.Name)
		if newColumn == nil {
			finding.Columns = append(finding.Columns, outputChange(oldColumn.Name, oldColumn.Type.String(), ""))
			continue
		}
		if arrow.TypeEqual(oldColumn.Type, newColumn.Type) {
			continue
		}
		column, err := cl.assessColumn(oldTable.Name, oldColumn, *newColumn)
		if err != nil {
			return plugin.TableFinding{}, false, err
		}
		finding.Columns = append(finding.Columns, column)
	}
	for _, newColumn := range pair.New.Columns {
		if oldTable.Columns.Get(newColumn.Name) == nil {
			finding.Columns = append(finding.Columns, outputChange(newColumn.Name, "", newColumn.Type.String()))
		}
	}
	if pair.Old == nil || slices.Equal(oldTable.Columns.Names(), pair.New.Columns.Names()) {
		return finding, false, nil
	}
	evidence, err := cl.rowEvidence(oldTable, pair.New)
	if err != nil {
		return plugin.TableFinding{}, false, err
	}
	finding.Evidence = append(finding.Evidence, evidence)
	existingColumns, err := cl.rowEvidence(oldTable, retainedColumns(oldTable, pair.New))
	if err != nil {
		return plugin.TableFinding{}, false, err
	}
	return finding, existingColumns.Before != existingColumns.After, nil
}

func retainedColumns(oldTable, newTable *schema.Table) *schema.Table {
	retained := *newTable
	retained.Columns = slices.DeleteFunc(slices.Clone(newTable.Columns), func(column schema.Column) bool {
		return oldTable.Columns.Get(column.Name) == nil
	})
	return &retained
}

func (cl *Client) isAdditiveColumn(pair plugin.TablePair, name string) bool {
	if pair.Old == nil {
		return true
	}
	if column := pair.New.Columns.Get(name); column == nil || column.NotNull {
		return false
	}
	csvClient, ok := cl.filetype.(*csvfile.Client)
	if !ok {
		return true
	}
	oldNames, newNames := pair.Old.Columns.Names(), pair.New.Columns.Names()
	return csvClient.IncludeHeaders && len(newNames) >= len(oldNames) && slices.Equal(newNames[:len(oldNames)], oldNames)
}

func classifyTable(finding *plugin.TableFinding, pair plugin.TablePair, outputChanged bool) {
	finding.Category = plugin.AssessCategoryNoChange
	var unknownColumns []string
	for _, column := range finding.Columns {
		switch column.Category {
		case plugin.AssessCategoryFileSchemaChanged:
			outputChanged = true
		case plugin.AssessCategoryAutomaticallyMigratable:
			finding.Category = plugin.AssessCategoryAutomaticallyMigratable
		case plugin.AssessCategoryUnknown:
			unknownColumns = append(unknownColumns, column.ColumnName)
		}
	}
	switch {
	case outputChanged:
		finding.Category = plugin.AssessCategoryFileSchemaChanged
	case finding.Category == plugin.AssessCategoryAutomaticallyMigratable && pair.Old == nil:
		finding.SafeModeBehavior, finding.ForcedModeBehavior = behaviorNewTable, behaviorNewTable
	case finding.Category == plugin.AssessCategoryAutomaticallyMigratable:
		finding.SafeModeBehavior, finding.ForcedModeBehavior = behaviorNewColumns, behaviorNewColumns
	}
	if len(unknownColumns) > 0 {
		if finding.Category == plugin.AssessCategoryNoChange {
			finding.Category = plugin.AssessCategoryUnknown
		}
		finding.IncompleteCoverageReason = "unable to compare: no equivalent values for columns " + strings.Join(unknownColumns, ", ")
	}
}

func outputChange(name, oldType, newType string) plugin.ColumnFinding {
	return plugin.ColumnFinding{
		ColumnName: name,
		Category:   plugin.AssessCategoryFileSchemaChanged,
		OldType:    oldType,
		NewType:    newType,
	}
}

func (cl *Client) assessColumn(tableName string, oldColumn, newColumn schema.Column) (plugin.ColumnFinding, error) {
	finding := plugin.ColumnFinding{
		ColumnName: oldColumn.Name,
		Category:   plugin.AssessCategoryNoChange,
		OldType:    oldColumn.Type.String(),
		NewType:    newColumn.Type.String(),
	}
	oldField, newField := oldColumn.ToArrowField(), newColumn.ToArrowField()
	pairs, err := schema.SyntheticPairs(oldField, newField)
	if errors.Is(err, schema.ErrUnableToCompare) {
		finding.Category = plugin.AssessCategoryUnknown
		return finding, nil
	}
	if err != nil {
		return plugin.ColumnFinding{}, err
	}
	oldTable := &schema.Table{Name: tableName, Columns: schema.ColumnList{oldColumn}}
	newTable := &schema.Table{Name: tableName, Columns: schema.ColumnList{newColumn}}
	for _, pair := range pairs {
		oldRecord, newRecord, err := schema.SyntheticRecords(oldField, newField, []schema.SyntheticPair{pair})
		if err != nil {
			return plugin.ColumnFinding{}, err
		}
		before, beforeErr := cl.serialize(oldTable, oldRecord)
		after, afterErr := cl.serialize(newTable, newRecord)
		oldRecord.Release()
		newRecord.Release()
		if beforeErr != nil {
			return plugin.ColumnFinding{}, beforeErr
		}
		if afterErr != nil {
			return plugin.ColumnFinding{}, afterErr
		}
		finding.Evidence = append(finding.Evidence, plugin.Evidence{SyntheticValue: pair.Value, Before: before, After: after})
		if before != after {
			finding.Category = plugin.AssessCategoryFileSchemaChanged
		}
	}
	return finding, nil
}

func (cl *Client) rowEvidence(oldTable, newTable *schema.Table) (plugin.Evidence, error) {
	values := make(map[string]string)
	for _, column := range append(slices.Clone(oldTable.Columns), newTable.Columns...) {
		if _, ok := values[column.Name]; ok {
			continue
		}
		values[column.Name] = "null"
		if pairs, err := schema.SyntheticPairs(fieldIn(oldTable, column), fieldIn(newTable, column)); err == nil {
			values[column.Name] = pairs[0].Value
		}
	}
	before, err := cl.serializeRow(oldTable, values)
	if err != nil {
		return plugin.Evidence{}, err
	}
	after, err := cl.serializeRow(newTable, values)
	if err != nil {
		return plugin.Evidence{}, err
	}
	return plugin.Evidence{Before: before, After: after}, nil
}

func fieldIn(table *schema.Table, fallback schema.Column) arrow.Field {
	if column := table.Columns.Get(fallback.Name); column != nil {
		return column.ToArrowField()
	}
	return fallback.ToArrowField()
}

func (cl *Client) serializeRow(table *schema.Table, values map[string]string) (string, error) {
	fields := make([]string, len(table.Columns))
	for i, column := range table.Columns {
		name, err := json.Marshal(column.Name)
		if err != nil {
			return "", err
		}
		fields[i] = string(name) + ":" + values[column.Name]
	}
	record, _, err := array.RecordFromJSON(memory.DefaultAllocator, table.ToArrowSchema(), strings.NewReader("[{"+strings.Join(fields, ",")+"}]"))
	if err != nil {
		return "", err
	}
	defer record.Release()
	return cl.serialize(table, record)
}

func (cl *Client) serialize(table *schema.Table, record arrow.RecordBatch) (string, error) {
	var b bytes.Buffer
	if err := types.WriteAll(cl.filetype, &b, table, []arrow.RecordBatch{record}); err != nil {
		return "", err
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}
