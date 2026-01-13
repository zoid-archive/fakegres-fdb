package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/google/uuid"
	pgquery "github.com/pganalyze/pg_query_go/v5"
)

type pgEngine struct {
	db fdb.Transactor
}

func newPgEngine(db fdb.Transactor) pgEngine {
	return pgEngine{db}
}

func (pe pgEngine) execute(tree pgquery.ParseResult) error {
	for _, stmt := range tree.GetStmts() {
		n := stmt.GetStmt()
		if c := n.GetCreateStmt(); c != nil {
			return pe.executeCreate(c)
		}

		if c := n.GetInsertStmt(); c != nil {
			return pe.executeInsert(c)
		}

		if c := n.GetDeleteStmt(); c != nil {
			return pe.executeDelete(c)
		}

		if c := n.GetUpdateStmt(); c != nil {
			return pe.executeUpdate(c)
		}

		if c := n.GetDropStmt(); c != nil {
			return pe.executeDrop(c)
		}

		if c := n.GetSelectStmt(); c != nil {
			_, err := pe.executeSelect(c)
			return err
		}
	}

	return nil
}

type tableDefinition struct {
	Name        string
	ColumnNames []string
	ColumnTypes []string
}

/*
Parse the create table SQL statement and create an equivalent KV structure in the database.

Example:

# The following SQL

```sql
create table user (age int, name text);
```

# Will produce the following KV structure

```
catalog/table/user: "" (empty value to mark that the table exists)
catalog/table/user/age: int
catalog/table/user/name: text
```

Keys in FoundationDB are globally sorted, so retrieving all the metadata for a table is
usually a single query.
*/
func (pe pgEngine) executeCreate(stmt *pgquery.CreateStmt) error {
	tbl := tableDefinition{}
	tbl.Name = stmt.Relation.Relname

	catalogDir, err := directory.CreateOrOpen(pe.db, []string{"catalog"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableSS := catalogDir.Sub("table")
	tableKey := tableSS.Pack(tuple.Tuple{tbl.Name})

	_, err = pe.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {

		if tr.Get(tableKey).MustGet() != nil {
			log.Printf("Table %s already exists", tbl.Name)
			return
		}

		// Note: table exists, marked by empty value and table name as key
		tr.Set(tableSS.Pack(tuple.Tuple{tbl.Name}), []byte(""))

		for _, c := range stmt.TableElts {
			cd := c.GetColumnDef()

			// Names is namespaced. So `INT` is pg_catalog.int4. `BIGINT` is pg_catalog.int8.
			var columnType string
			for _, n := range cd.TypeName.Names {
				if columnType != "" {
					columnType += "."
				}
				columnType += n.GetString_().GetSval()
			}
			tr.Set(tableSS.Pack(tuple.Tuple{tbl.Name, cd.Colname}), []byte(columnType))
		}

		return
	})

	if err != nil {
		return fmt.Errorf("could not create table: %s", err)
	}

	return nil
}

/*

Get the table definition from the database. This can be done with a single range query.

*/

func (pe pgEngine) getTableDefinition(name string) (*tableDefinition, error) {
	var tbl tableDefinition

	// TODO: check if table exists, etc.
	tbl.Name = name

	catalogDir, err := directory.CreateOrOpen(pe.db, []string{"catalog"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	tableSS := catalogDir.Sub("table")

	_, err = pe.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		ri := rtr.GetRange(tableSS.Sub(name), fdb.RangeOptions{
			Mode: fdb.StreamingModeWantAll,
		}).Iterator()
		for ri.Advance() {
			kv := ri.MustGet()
			t, _ := tableSS.Unpack(kv.Key)

			// Note: deconstruct the key from catalog/table/user/age and extract the column name
			tbl.ColumnNames = append(tbl.ColumnNames, t[1].(string))
			tbl.ColumnTypes = append(tbl.ColumnTypes, string(kv.Value))
		}
		return nil, nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not get table defn: %s", err)
	}
	return &tbl, err
}

/*

Parse the insert statement and insert data into the table.

Example:

The following SQL

```sql
insert into user values(14, 'garry'), (20, 'ted');
```

Note that since keys are sorted, CREATE TABLE and positional ORDER of INSERT can be different.

Will produce the following KV structure

```
data/table_data/user/age/72746a7f-727f-4e0a-88f1-d983fea5c158: 14
data/table_data/user/age/34e7ff77-1bed-4ebd-be56-4b966e67c595: 20
data/table_data/user/name/72746a7f-727f-4e0a-88f1-d983fea5c158: garry
data/table_data/user/name/34e7ff77-1bed-4ebd-be56-4b966e67c595: ted
```

Keys in FoundationDB are globally sorted, so the data for this table would be a single query.
However, in this structure (column first) we will receive the table cells in age, age, name, name order and we will need to
collect them in order in select.

This property of Foundation DB is very interesting. Quoting the docs: https://apple.github.io/foundationdb/data-modeling.html
> You can make your model row-oriented or column-oriented by placing either the row or column first
> in the tuple, respectively. Because the lexicographic order sorts tuple elements from left to right,
> access is optimized for the element placed first. Placing the row first makes it efficient to read all
> the cells in a particular row; reversing the order makes reading a column more efficient.

We can insert columnar and row based data in the same table in same transaction and still be able to read them efficiently.
Note for future.

If this was row based, the keys in the database would be:

data/table_data/user/age/72746a7f-727f-4e0a-88f1-d983fea5c158: 14
data/table_data/user/name/72746a7f-727f-4e0a-88f1-d983fea5c158: garry
data/table_data/user/age/34e7ff77-1bed-4ebd-be56-4b966e67c595: 20
data/table_data/user/name/34e7ff77-1bed-4ebd-be56-4b966e67c595: ted

And reading them in select would be easier.
*/

func (pe pgEngine) executeInsert(stmt *pgquery.InsertStmt) error {
	tblName := stmt.Relation.Relname
	slct := stmt.GetSelectStmt().GetSelectStmt()

	tbl, err := pe.getTableDefinition(tblName)
	if err != nil {
		return err
	}

	// Determine column order: use explicit list or default to table definition
	var columnOrder []string
	if len(stmt.Cols) > 0 {
		// Explicit column list: INSERT INTO tbl (col1, col2) VALUES ...
		for _, col := range stmt.Cols {
			resTarget := col.GetResTarget()
			if resTarget != nil {
				columnOrder = append(columnOrder, resTarget.Name)
			}
		}
	} else {
		// Use table column order
		columnOrder = tbl.ColumnNames
	}

	catalogDir, err := directory.CreateOrOpen(pe.db, []string{"catalog"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableSS := catalogDir.Sub("table")
	tableKey := tableSS.Pack(tuple.Tuple{tblName})

	dataDir, err := directory.CreateOrOpen(pe.db, []string{"data"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableDataSS := dataDir.Sub("table_data")

	_, err = pe.db.Transact(func(tr fdb.Transaction) (ret interface{}, err error) {
		if tr.Get(tableKey).MustGet() == nil {
			log.Printf("Table %s does not exist", tblName)
			return
		}

		for _, values := range slct.ValuesLists {
			id := uuid.New().String()
			items := values.GetList().Items

			for columnIndex, value := range items {
				if columnIndex >= len(columnOrder) {
					break
				}
				colName := columnOrder[columnIndex]

				if c := value.GetAConst(); c != nil {
					var valBytes []byte

					if s := c.GetSval(); s != nil {
						valBytes = []byte(s.GetSval())
					} else if i := c.GetIval(); i != nil {
						valBytes, _ = json.Marshal(i.GetIval())
					} else if c.GetIsnull() {
						valBytes = nil // NULL value
					} else if b := c.GetBoolval(); b != nil {
						if b.GetBoolval() {
							valBytes = []byte("true")
						} else {
							valBytes = []byte("false")
						}
					} else {
						return nil, fmt.Errorf("unsupported constant type")
					}

					// Columnar data
					tr.Set(tableDataSS.Pack(tuple.Tuple{tblName, "c", colName, id}), valBytes)
					// Row based data
					tr.Set(tableDataSS.Pack(tuple.Tuple{tblName, "r", id, colName}), valBytes)
					continue
				}

				return nil, fmt.Errorf("unknown value type: %s", value)
			}
		}
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("could not insert into the table table: %s", err)
	}

	return nil
}

/*

Parse the delete statement and delete data from the table.
Currently, this doesn't support where clause and deletes all the data from the table.

*/

func (pe pgEngine) executeDelete(stmt *pgquery.DeleteStmt) error {
	tblName := stmt.Relation.Relname

	catalogDir, err := directory.CreateOrOpen(pe.db, []string{"catalog"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableSS := catalogDir.Sub("table")
	tableKey := tableSS.Pack(tuple.Tuple{tblName})

	dataDir, err := directory.CreateOrOpen(pe.db, []string{"data"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableDataSS := dataDir.Sub("table_data")

	_, err = pe.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if tr.Get(tableKey).MustGet() == nil {
			log.Printf("Table %s does not exist", tblName)
			return nil, nil
		}

		// If no WHERE clause, delete all rows
		if stmt.WhereClause == nil {
			query := tableDataSS.Pack(tuple.Tuple{tblName})
			rangeQuery, _ := fdb.PrefixRange(query)
			ri := tr.GetRange(rangeQuery, fdb.RangeOptions{
				Mode: fdb.StreamingModeWantAll,
			}).Iterator()
			for ri.Advance() {
				kv := ri.MustGet()
				tr.Clear(kv.Key)
			}
			return nil, nil
		}

		// With WHERE clause, we need to:
		// 1. Scan row-based data to find matching rows
		// 2. Delete both row and columnar entries for matching rows
		query := tableDataSS.Pack(tuple.Tuple{tblName, "r"})
		rangeQuery, _ := fdb.PrefixRange(query)
		ri := tr.GetRange(rangeQuery, fdb.RangeOptions{
			Mode: fdb.StreamingModeWantAll,
		}).Iterator()

		// Group values by row ID
		rowData := make(map[string]map[string]string)
		rowKeys := make(map[string][]fdb.Key) // Track keys for each row

		for ri.Advance() {
			kv := ri.MustGet()
			t, _ := tableDataSS.Unpack(kv.Key)

			currentInternalRowId := t[2].(string)
			currentColumnName := t[3].(string)

			if _, exists := rowData[currentInternalRowId]; !exists {
				rowData[currentInternalRowId] = make(map[string]string)
				rowKeys[currentInternalRowId] = []fdb.Key{}
			}
			rowData[currentInternalRowId][currentColumnName] = string(kv.Value)
			rowKeys[currentInternalRowId] = append(rowKeys[currentInternalRowId], kv.Key)
		}

		// Find rows matching WHERE and delete them
		for rowId, data := range rowData {
			match, err := evaluateWhereExpr(stmt.WhereClause, data)
			if err != nil {
				log.Printf("WHERE evaluation error: %v", err)
				continue
			}
			if match {
				// Delete row-based keys
				for _, key := range rowKeys[rowId] {
					tr.Clear(key)
				}
				// Delete columnar keys for this row
				for colName := range data {
					colKey := tableDataSS.Pack(tuple.Tuple{tblName, "c", colName, rowId})
					tr.Clear(colKey)
				}
			}
		}

		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("could not delete from table: %s", err)
	}
	return nil
}

func (pe pgEngine) executeUpdate(stmt *pgquery.UpdateStmt) error {
	tblName := stmt.Relation.Relname

	catalogDir, err := directory.CreateOrOpen(pe.db, []string{"catalog"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableSS := catalogDir.Sub("table")
	tableKey := tableSS.Pack(tuple.Tuple{tblName})

	dataDir, err := directory.CreateOrOpen(pe.db, []string{"data"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableDataSS := dataDir.Sub("table_data")

	// Parse SET clause to get column updates
	updates := make(map[string]string)
	for _, target := range stmt.TargetList {
		resTarget := target.GetResTarget()
		if resTarget == nil {
			continue
		}
		colName := resTarget.Name
		
		// Get the value from the expression
		if aConst := resTarget.Val.GetAConst(); aConst != nil {
			if sval := aConst.GetSval(); sval != nil {
				updates[colName] = sval.GetSval()
			} else if ival := aConst.GetIval(); ival != nil {
				updates[colName] = fmt.Sprintf("%d", ival.GetIval())
			}
		}
	}

	_, err = pe.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		if tr.Get(tableKey).MustGet() == nil {
			log.Printf("Table %s does not exist", tblName)
			return nil, nil
		}

		// Scan row-based data
		query := tableDataSS.Pack(tuple.Tuple{tblName, "r"})
		rangeQuery, _ := fdb.PrefixRange(query)
		ri := tr.GetRange(rangeQuery, fdb.RangeOptions{
			Mode: fdb.StreamingModeWantAll,
		}).Iterator()

		// Group values by row ID
		rowData := make(map[string]map[string]string)
		var rowOrder []string

		for ri.Advance() {
			kv := ri.MustGet()
			t, _ := tableDataSS.Unpack(kv.Key)

			currentInternalRowId := t[2].(string)
			currentColumnName := t[3].(string)

			if _, exists := rowData[currentInternalRowId]; !exists {
				rowData[currentInternalRowId] = make(map[string]string)
				rowOrder = append(rowOrder, currentInternalRowId)
			}
			rowData[currentInternalRowId][currentColumnName] = string(kv.Value)
		}

		// Find rows matching WHERE and update them
		for _, rowId := range rowOrder {
			data := rowData[rowId]

			// Check WHERE clause
			if stmt.WhereClause != nil {
				match, err := evaluateWhereExpr(stmt.WhereClause, data)
				if err != nil {
					log.Printf("WHERE evaluation error: %v", err)
					continue
				}
				if !match {
					continue
				}
			}

			// Apply updates
			for colName, newVal := range updates {
				// Update row-based storage
				rowKey := tableDataSS.Pack(tuple.Tuple{tblName, "r", rowId, colName})
				tr.Set(rowKey, []byte(newVal))
				// Update columnar storage
				colKey := tableDataSS.Pack(tuple.Tuple{tblName, "c", colName, rowId})
				tr.Set(colKey, []byte(newVal))
			}
		}

		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("could not update table: %s", err)
	}
	return nil
}

func (pe pgEngine) executeDrop(stmt *pgquery.DropStmt) error {
	// Only handle DROP TABLE for now
	if stmt.RemoveType != pgquery.ObjectType_OBJECT_TABLE {
		return fmt.Errorf("DROP only supported for tables")
	}

	for _, obj := range stmt.Objects {
		// Object is a List containing the table name
		list := obj.GetList()
		if list == nil || len(list.Items) == 0 {
			continue
		}
		tblName := list.Items[0].GetString_().GetSval()

		catalogDir, err := directory.CreateOrOpen(pe.db, []string{"catalog"}, nil)
		if err != nil {
			return err
		}
		tableSS := catalogDir.Sub("table")

		dataDir, err := directory.CreateOrOpen(pe.db, []string{"data"}, nil)
		if err != nil {
			return err
		}
		tableDataSS := dataDir.Sub("table_data")

		_, err = pe.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			// Check if table exists
			tableKey := tableSS.Pack(tuple.Tuple{tblName})
			if tr.Get(tableKey).MustGet() == nil {
				if !stmt.MissingOk {
					return nil, fmt.Errorf("table %s does not exist", tblName)
				}
				return nil, nil
			}

			// Delete table metadata from catalog
			catalogRange, _ := fdb.PrefixRange(tableSS.Pack(tuple.Tuple{tblName}))
			ri := tr.GetRange(catalogRange, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).Iterator()
			for ri.Advance() {
				tr.Clear(ri.MustGet().Key)
			}

			// Delete all table data
			dataRange, _ := fdb.PrefixRange(tableDataSS.Pack(tuple.Tuple{tblName}))
			ri = tr.GetRange(dataRange, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).Iterator()
			for ri.Advance() {
				tr.Clear(ri.MustGet().Key)
			}

			return nil, nil
		})
		if err != nil {
			return fmt.Errorf("could not drop table: %s", err)
		}
	}
	return nil
}

type pgResult struct {
	fieldNames []string
	fieldTypes []string
	rows       [][]any
}

// evaluateWhereExpr evaluates a WHERE expression against a row
// Returns true if the row matches the condition
func evaluateWhereExpr(expr *pgquery.Node, rowData map[string]string) (bool, error) {
	if expr == nil {
		return true, nil
	}

	// Handle A_Expr (comparison operators like =, <>, <, >, <=, >=)
	if aExpr := expr.GetAExpr(); aExpr != nil {
		// Get column name from left side
		leftColRef := aExpr.Lexpr.GetColumnRef()
		if leftColRef == nil || len(leftColRef.Fields) == 0 {
			return false, fmt.Errorf("left side of comparison must be a column")
		}
		colName := leftColRef.Fields[0].GetString_().GetSval()

		// Get value from right side
		rightConst := aExpr.Rexpr.GetAConst()
		if rightConst == nil {
			return false, fmt.Errorf("right side of comparison must be a constant")
		}

		var compareVal string
		if sval := rightConst.GetSval(); sval != nil {
			compareVal = sval.GetSval()
		} else if ival := rightConst.GetIval(); ival != nil {
			compareVal = fmt.Sprintf("%d", ival.GetIval())
		} else {
			return false, fmt.Errorf("unsupported constant type")
		}

		rowVal, ok := rowData[colName]
		if !ok {
			return false, fmt.Errorf("column %s not found", colName)
		}

		// Get operator name
		opName := ""
		if len(aExpr.Name) > 0 {
			opName = aExpr.Name[0].GetString_().GetSval()
		}

		switch opName {
		case "=":
			return rowVal == compareVal, nil
		case "<>", "!=":
			return rowVal != compareVal, nil
		case "<":
			return rowVal < compareVal, nil
		case ">":
			return rowVal > compareVal, nil
		case "<=":
			return rowVal <= compareVal, nil
		case ">=":
			return rowVal >= compareVal, nil
		default:
			return false, fmt.Errorf("unsupported operator: %s", opName)
		}
	}

	// Handle BoolExpr (AND, OR, NOT)
	if boolExpr := expr.GetBoolExpr(); boolExpr != nil {
		switch boolExpr.Boolop {
		case pgquery.BoolExprType_AND_EXPR:
			for _, arg := range boolExpr.Args {
				result, err := evaluateWhereExpr(arg, rowData)
				if err != nil {
					return false, err
				}
				if !result {
					return false, nil
				}
			}
			return true, nil
		case pgquery.BoolExprType_OR_EXPR:
			for _, arg := range boolExpr.Args {
				result, err := evaluateWhereExpr(arg, rowData)
				if err != nil {
					return false, err
				}
				if result {
					return true, nil
				}
			}
			return false, nil
		case pgquery.BoolExprType_NOT_EXPR:
			if len(boolExpr.Args) > 0 {
				result, err := evaluateWhereExpr(boolExpr.Args[0], rowData)
				if err != nil {
					return false, err
				}
				return !result, nil
			}
		}
	}

	return false, fmt.Errorf("unsupported WHERE expression type")
}

/*

Parse the select statement and return the result.

Example:

The following SQL:

```sql
select name, age from customer;
```

Will produce the following KV structure:

```
data/table_data/user/age/72746a7f-727f-4e0a-88f1-d983fea5c158: 14
data/table_data/user/age/34e7ff77-1bed-4ebd-be56-4b966e67c595: 20
data/table_data/user/name/72746a7f-727f-4e0a-88f1-d983fea5c158: garry
data/table_data/user/name/34e7ff77-1bed-4ebd-be56-4b966e67c595: ted
```

The Select code collects them into [[14, garry], [20, ted]] and returns the result accordingly.
*/

func (pe pgEngine) executeSelectColumnar(stmt *pgquery.SelectStmt) (*pgResult, error) {
	tblName := stmt.FromClause[0].GetRangeVar().Relname
	tbl, err := pe.getTableDefinition(tblName)
	if err != nil {
		return nil, err
	}

	results := &pgResult{}
	for _, c := range stmt.TargetList {
		fieldName := c.GetResTarget().Val.GetColumnRef().Fields[0].GetString_().GetSval()
		results.fieldNames = append(results.fieldNames, fieldName)

		fieldType := ""
		for i, cn := range tbl.ColumnNames {
			if cn == fieldName {
				fieldType = tbl.ColumnTypes[i]
			}
		}

		if fieldType == "" {
			return nil, fmt.Errorf("unknown field: %s", fieldName)
		}

		results.fieldTypes = append(results.fieldTypes, fieldType)
	}

	dataDir, err := directory.CreateOrOpen(pe.db, []string{"data"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableDataSS := dataDir.Sub("table_data")

	_, _ = pe.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		query := tableDataSS.Pack(tuple.Tuple{tbl.Name, "c"})
		rangeQuery, _ := fdb.PrefixRange(query)
		ri := tr.GetRange(rangeQuery, fdb.RangeOptions{
			Mode: fdb.StreamingModeWantAll,
		}).Iterator()

		var columnOrder []string
		var targetRows [][]any
		targetRows = append(targetRows, []any{})
		rowIndex := -1
		lastColumn := ""
		for ri.Advance() {
			kv := ri.MustGet()
			t, _ := tableDataSS.Unpack(kv.Key)

			currentTableName := t[0].(string)
			currentColumnFormat := t[1].(string)
			currentColumnName := t[2].(string)
			currentInternalRowId := t[3].(string)
			log.Println("fetching row metadata: ", currentTableName, currentColumnFormat, currentColumnName, currentInternalRowId)
			if currentColumnName != lastColumn {
				rowIndex = 0
				lastColumn = currentColumnName
				columnOrder = append(columnOrder, currentColumnName)
			} else {
				targetRows = append(targetRows, []any{})
			}

			for _, target := range results.fieldNames {
				if target == currentColumnName {
					targetRows[rowIndex] = append(targetRows[rowIndex], string(kv.Value))
				}
			}
			rowIndex += 1
		}
		results.fieldNames = columnOrder

		// TODO: don't add empty arrays in the first place
		var targetRowsFinal [][]any
		targetRows = append(targetRows, []any{})
		for _, row := range targetRows {
			if len(row) > 0 {
				targetRowsFinal = append(targetRowsFinal, row)
			}
		}
		results.rows = targetRowsFinal
		return results, nil
	})

	return results, nil
}

func (pe pgEngine) executeSelect(stmt *pgquery.SelectStmt) (*pgResult, error) {
	tblName := stmt.FromClause[0].GetRangeVar().Relname
	tbl, err := pe.getTableDefinition(tblName)
	if err != nil {
		return nil, err
	}

	results := &pgResult{}

	// Check for SELECT * (ColumnRef with A_Star)
	isSelectStar := false
	if len(stmt.TargetList) == 1 {
		target := stmt.TargetList[0].GetResTarget()
		if target != nil && target.Val != nil {
			colRef := target.Val.GetColumnRef()
			if colRef != nil && len(colRef.Fields) == 1 {
				if colRef.Fields[0].GetAStar() != nil {
					isSelectStar = true
				}
			}
		}
	}

	if isSelectStar {
		// SELECT * - use all columns from table definition
		results.fieldNames = append(results.fieldNames, tbl.ColumnNames...)
		results.fieldTypes = append(results.fieldTypes, tbl.ColumnTypes...)
	} else {
		// Explicit column list
		for _, c := range stmt.TargetList {
			fieldName := c.GetResTarget().Val.GetColumnRef().Fields[0].GetString_().GetSval()
			results.fieldNames = append(results.fieldNames, fieldName)

			fieldType := ""
			for i, cn := range tbl.ColumnNames {
				if cn == fieldName {
					fieldType = tbl.ColumnTypes[i]
				}
			}

			if fieldType == "" {
				return nil, fmt.Errorf("unknown field: %s", fieldName)
			}

			results.fieldTypes = append(results.fieldTypes, fieldType)
		}
	}

	dataDir, err := directory.CreateOrOpen(pe.db, []string{"data"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	tableDataSS := dataDir.Sub("table_data")

	// Build a map of requested column name -> index in result
	requestedColIndex := make(map[string]int)
	for i, name := range results.fieldNames {
		requestedColIndex[name] = i
	}

	_, _ = pe.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		query := tableDataSS.Pack(tuple.Tuple{tbl.Name, "r"})
		rangeQuery, _ := fdb.PrefixRange(query)
		ri := tr.GetRange(rangeQuery, fdb.RangeOptions{
			Mode: fdb.StreamingModeWantAll,
		}).Iterator()

		// Group values by row ID
		rowData := make(map[string]map[string]string)
		var rowOrder []string

		for ri.Advance() {
			kv := ri.MustGet()
			t, _ := tableDataSS.Unpack(kv.Key)

			currentInternalRowId := t[2].(string)
			currentColumnName := t[3].(string)

			if _, exists := rowData[currentInternalRowId]; !exists {
				rowData[currentInternalRowId] = make(map[string]string)
				rowOrder = append(rowOrder, currentInternalRowId)
			}
			rowData[currentInternalRowId][currentColumnName] = string(kv.Value)
		}

		// Build result rows in the requested column order, applying WHERE filter
		for _, rowId := range rowOrder {
			// Check WHERE clause
			if stmt.WhereClause != nil {
				match, err := evaluateWhereExpr(stmt.WhereClause, rowData[rowId])
				if err != nil {
					log.Printf("WHERE evaluation error: %v", err)
					continue
				}
				if !match {
					continue
				}
			}

			row := make([]any, len(results.fieldNames))
			for colName, val := range rowData[rowId] {
				if idx, ok := requestedColIndex[colName]; ok {
					row[idx] = val
				}
			}
			results.rows = append(results.rows, row)
		}

		return results, nil
	})

	// Apply ORDER BY
	if len(stmt.SortClause) > 0 {
		sortBy := stmt.SortClause[0].GetSortBy()
		if sortBy != nil {
			sortColName := sortBy.Node.GetColumnRef().Fields[0].GetString_().GetSval()
			sortColIdx := -1
			for i, name := range results.fieldNames {
				if name == sortColName {
					sortColIdx = i
					break
				}
			}

			if sortColIdx >= 0 {
				isDesc := sortBy.SortbyDir == pgquery.SortByDir_SORTBY_DESC

				sort.SliceStable(results.rows, func(i, j int) bool {
					vi := fmt.Sprintf("%v", results.rows[i][sortColIdx])
					vj := fmt.Sprintf("%v", results.rows[j][sortColIdx])

					// Try numeric comparison first
					ni, errI := strconv.ParseFloat(vi, 64)
					nj, errJ := strconv.ParseFloat(vj, 64)
					if errI == nil && errJ == nil {
						if isDesc {
							return ni > nj
						}
						return ni < nj
					}

					// Fall back to string comparison
					if isDesc {
						return vi > vj
					}
					return vi < vj
				})
			}
		}
	}

	// Apply LIMIT
	if stmt.LimitCount != nil {
		if limitConst := stmt.LimitCount.GetAConst(); limitConst != nil {
			if limitVal := limitConst.GetIval(); limitVal != nil {
				limit := int(limitVal.GetIval())
				if limit < len(results.rows) {
					results.rows = results.rows[:limit]
				}
			}
		}
	}

	// Apply OFFSET
	if stmt.LimitOffset != nil {
		if offsetConst := stmt.LimitOffset.GetAConst(); offsetConst != nil {
			if offsetVal := offsetConst.GetIval(); offsetVal != nil {
				offset := int(offsetVal.GetIval())
				if offset < len(results.rows) {
					results.rows = results.rows[offset:]
				} else {
					results.rows = nil
				}
			}
		}
	}

	return results, nil
}
