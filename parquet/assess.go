package parquet

import (
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	pqschema "github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

// ParquetSchema returns the Parquet schema WriteHeader writes for the table, without writing anything.
func (c *Client) ParquetSchema(t *schema.Table) (*pqschema.Schema, error) {
	return pqarrow.ToParquet(convertSchema(t.ToArrowSchema()), c.writerProps(), pqarrow.DefaultWriterProps())
}

// AssessTable compares the Parquet schemas generated for the old and new table.
func (c *Client) AssessTable(pair plugin.TablePair) (plugin.TableFinding, error) {
	finding := plugin.TableFinding{TableName: pair.TableName(), Category: plugin.AssessCategoryNoChange}
	if pair.New == nil {
		finding.Category = plugin.AssessCategoryTableRemoved
		return finding, nil
	}
	oldNames, oldTypes, err := c.fieldTypes(pair.Old)
	if err != nil {
		return plugin.TableFinding{}, err
	}
	newNames, newTypes, err := c.fieldTypes(pair.New)
	if err != nil {
		return plugin.TableFinding{}, err
	}
	for _, name := range oldNames {
		if oldTypes[name] != newTypes[name] {
			finding.Columns = append(finding.Columns, fileSchemaChange(name, oldTypes[name], newTypes[name]))
		}
	}
	for _, name := range newNames {
		if _, ok := oldTypes[name]; !ok {
			finding.Columns = append(finding.Columns, fileSchemaChange(name, "", newTypes[name]))
		}
	}
	if len(finding.Columns) > 0 {
		finding.Category = plugin.AssessCategoryFileSchemaChanged
	}
	return finding, nil
}

func fileSchemaChange(name, oldType, newType string) plugin.ColumnFinding {
	return plugin.ColumnFinding{
		ColumnName: name,
		Category:   plugin.AssessCategoryFileSchemaChanged,
		OldType:    oldType,
		NewType:    newType,
	}
}

func (c *Client) fieldTypes(t *schema.Table) ([]string, map[string]string, error) {
	typesByName := make(map[string]string)
	if t == nil {
		return nil, typesByName, nil
	}
	sc, err := c.ParquetSchema(t)
	if err != nil {
		return nil, nil, err
	}
	root := sc.Root()
	names := make([]string, root.NumFields())
	for i := range names {
		field := root.Field(i)
		names[i] = field.Name()
		typesByName[field.Name()] = parquetType(field)
	}
	return names, typesByName, nil
}

func parquetType(node pqschema.Node) string {
	repetition := strings.ToLower(node.RepetitionType().String())
	logicalType := ""
	if lt := node.LogicalType(); lt != nil && !lt.IsNone() {
		logicalType = " (" + lt.String() + ")"
	}
	group, ok := node.(*pqschema.GroupNode)
	if !ok {
		primitive := node.(*pqschema.PrimitiveNode)
		physicalType := strings.ToLower(primitive.PhysicalType().String())
		if primitive.PhysicalType() == parquet.Types.FixedLenByteArray {
			physicalType += "(" + strconv.Itoa(primitive.TypeLength()) + ")"
		}
		return repetition + " " + physicalType + logicalType
	}
	children := make([]string, group.NumFields())
	for i := range children {
		children[i] = group.Field(i).Name() + ": " + parquetType(group.Field(i))
	}
	return repetition + " group" + logicalType + " {" + strings.Join(children, "; ") + "}"
}
