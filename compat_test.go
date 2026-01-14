//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/jackc/pgx/v5"
)

func TestMain(m *testing.M) {
	host := os.Getenv("FAKEGRES_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("FAKEGRES_PORT")
	if port == "" {
		port = "6000"
	}

	addr := net.JoinHostPort(host, port)
	conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err == nil {
		conn.Close()
		os.Exit(m.Run())
	}

	fdb.MustAPIVersion(710)
	db := fdb.MustOpenDefault()

	db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(fdb.KeyRange{Begin: fdb.Key{}, End: fdb.Key{0xFF}})
		return nil, nil
	})

	cfg := config{pgPort: port}
	go runPgServer(port, db, cfg)

	for i := 0; i < 50; i++ {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	os.Exit(m.Run())
}

type QueryKind string

const (
	KindQuery QueryKind = "query"
	KindExec  QueryKind = "exec"
)

type SQLTestCase struct {
	Name     string
	Kind     QueryKind
	SetupSQL []string
	MainSQL  string
	Tags     []string
	Skip     string // reason to skip, empty means don't skip
}

var testCases = []SQLTestCase{
	// DDL - CREATE TABLE
	{
		Name:     "create_table_simple",
		Kind:     KindExec,
		SetupSQL: []string{},
		MainSQL:  "CREATE TABLE users (id int, name text)",
		Tags:     []string{"ddl", "create"},
	},
	{
		Name:     "create_table_multiple_columns",
		Kind:     KindExec,
		SetupSQL: []string{},
		MainSQL:  "CREATE TABLE products (id int, name text, price int, description text)",
		Tags:     []string{"ddl", "create"},
	},

	// DML - INSERT
	{
		Name:     "insert_single_row",
		Kind:     KindExec,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)"},
		MainSQL:  "INSERT INTO users VALUES (1, 'alice')",
		Tags:     []string{"dml", "insert"},
	},
	{
		Name:     "insert_multiple_rows",
		Kind:     KindExec,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)"},
		MainSQL:  "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
		Tags:     []string{"dml", "insert"},
	},

	// DML - SELECT basic
	{
		Name:     "select_all_columns",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')"},
		MainSQL:  "SELECT id, name FROM users",
		Tags:     []string{"dml", "select"},
	},
	{
		Name:     "select_single_column",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')"},
		MainSQL:  "SELECT name FROM users",
		Tags:     []string{"dml", "select"},
	},
	{
		Name:     "select_reorder_columns",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')"},
		MainSQL:  "SELECT name, id FROM users",
		Tags:     []string{"dml", "select"},
	},
	{
		Name:     "select_star",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice')"},
		MainSQL:  "SELECT * FROM users",
		Tags:     []string{"dml", "select", "select-star"},
	},

	// DML - DELETE
	{
		Name:     "delete_all",
		Kind:     KindExec,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')"},
		MainSQL:  "DELETE FROM users",
		Tags:     []string{"dml", "delete"},
	},
	{
		Name:     "delete_where_eq",
		Kind:     KindExec,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')"},
		MainSQL:  "DELETE FROM users WHERE id = 1",
		Tags:     []string{"dml", "delete", "where"},
	},

	// DML - UPDATE
	{
		Name:     "update_all",
		Kind:     KindExec,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice')"},
		MainSQL:  "UPDATE users SET name = 'updated'",
		Tags:     []string{"dml", "update"},
	},
	{
		Name:     "update_where_eq",
		Kind:     KindExec,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')"},
		MainSQL:  "UPDATE users SET name = 'updated' WHERE id = 1",
		Tags:     []string{"dml", "update", "where"},
	},

	// WHERE clauses
	{
		Name:     "select_where_eq_int",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')"},
		MainSQL:  "SELECT id, name FROM users WHERE id = 1",
		Tags:     []string{"dml", "select", "where"},
	},
	{
		Name:     "select_where_eq_text",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')"},
		MainSQL:  "SELECT id, name FROM users WHERE name = 'alice'",
		Tags:     []string{"dml", "select", "where"},
	},
	{
		Name:     "select_where_neq",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')"},
		MainSQL:  "SELECT id, name FROM users WHERE id <> 1",
		Tags:     []string{"dml", "select", "where"},
	},
	{
		Name:     "select_where_gt",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')"},
		MainSQL:  "SELECT id, name FROM users WHERE id > 1",
		Tags:     []string{"dml", "select", "where"},
	},
	{
		Name:     "select_where_lt",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')"},
		MainSQL:  "SELECT id, name FROM users WHERE id < 3",
		Tags:     []string{"dml", "select", "where"},
	},
	{
		Name:     "select_where_and",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'alice')"},
		MainSQL:  "SELECT id, name FROM users WHERE id > 1 AND name = 'alice'",
		Tags:     []string{"dml", "select", "where"},
	},
	{
		Name:     "select_where_or",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')"},
		MainSQL:  "SELECT id, name FROM users WHERE id = 1 OR id = 3",
		Tags:     []string{"dml", "select", "where"},
	},

	// DDL - DROP TABLE
	{
		Name:     "drop_table",
		Kind:     KindExec,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)"},
		MainSQL:  "DROP TABLE users",
		Tags:     []string{"ddl", "drop"},
	},

	// ORDER BY
	{
		Name:     "select_order_by_asc",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (3, 'charlie'), (1, 'alice'), (2, 'bob')"},
		MainSQL:  "SELECT id, name FROM users ORDER BY id ASC",
		Tags:     []string{"dml", "select", "order-by"},
	},
	{
		Name:     "select_order_by_desc",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')"},
		MainSQL:  "SELECT id, name FROM users ORDER BY id DESC",
		Tags:     []string{"dml", "select", "order-by"},
	},

	// LIMIT/OFFSET
	{
		Name:     "select_limit",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')"},
		MainSQL:  "SELECT id, name FROM users LIMIT 2",
		Tags:     []string{"dml", "select", "limit"},
	},
	{
		Name:     "select_offset",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')"},
		MainSQL:  "SELECT id, name FROM users LIMIT 2 OFFSET 1",
		Tags:     []string{"dml", "select", "limit", "offset"},
	},

	// Expressions
	{
		Name:     "select_literal_int",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice')"},
		MainSQL:  "SELECT 1",
		Tags:     []string{"dml", "select", "expression"},
		Skip:     "Literal expressions not implemented",
	},
	{
		Name:     "select_literal_text",
		Kind:     KindQuery,
		SetupSQL: []string{},
		MainSQL:  "SELECT 'hello'",
		Tags:     []string{"dml", "select", "expression"},
		Skip:     "Literal expressions not implemented",
	},

	// Column aliases
	{
		Name:     "select_alias",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice')"},
		MainSQL:  "SELECT id AS user_id, name AS user_name FROM users",
		Tags:     []string{"dml", "select", "alias"},
		Skip:     "Column aliases not implemented",
	},

	// COUNT aggregate
	{
		Name:     "select_count_star",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')"},
		MainSQL:  "SELECT COUNT(*) FROM users",
		Tags:     []string{"dml", "select", "aggregate"},
		Skip:     "Aggregates not implemented",
	},

	// NULL handling
	{
		Name:     "insert_null",
		Kind:     KindExec,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)"},
		MainSQL:  "INSERT INTO users VALUES (1, NULL)",
		Tags:     []string{"dml", "insert", "null"},
	},

	// INSERT with column list
	{
		Name:     "insert_with_columns",
		Kind:     KindExec,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)"},
		MainSQL:  "INSERT INTO users (name, id) VALUES ('alice', 1)",
		Tags:     []string{"dml", "insert"},
	},

	// Boolean type
	{
		Name:     "create_table_bool",
		Kind:     KindExec,
		SetupSQL: []string{},
		MainSQL:  "CREATE TABLE flags (id int, active bool)",
		Tags:     []string{"ddl", "create", "types"},
	},

	// TRUNCATE
	{
		Name:     "truncate_table",
		Kind:     KindExec,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice')"},
		MainSQL:  "TRUNCATE TABLE users",
		Tags:     []string{"ddl", "truncate"},
	},

	// Combined operations
	{
		Name:     "insert_select_delete_cycle",
		Kind:     KindQuery,
		SetupSQL: []string{
			"CREATE TABLE users (id int, name text)",
			"INSERT INTO users VALUES (1, 'alice'), (2, 'bob')",
			"DELETE FROM users WHERE id = 1",
		},
		MainSQL: "SELECT id, name FROM users",
		Tags:    []string{"dml", "select", "delete", "combined"},
	},
	{
		Name:     "update_then_select",
		Kind:     KindQuery,
		SetupSQL: []string{
			"CREATE TABLE users (id int, name text)",
			"INSERT INTO users VALUES (1, 'alice')",
			"UPDATE users SET name = 'updated' WHERE id = 1",
		},
		MainSQL: "SELECT name FROM users WHERE id = 1",
		Tags:    []string{"dml", "select", "update", "combined"},
	},
	{
		Name:     "select_where_gte",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')"},
		MainSQL:  "SELECT id, name FROM users WHERE id >= 2",
		Tags:     []string{"dml", "select", "where"},
	},
	{
		Name:     "select_where_lte",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')"},
		MainSQL:  "SELECT id, name FROM users WHERE id <= 2",
		Tags:     []string{"dml", "select", "where"},
	},
	{
		Name:     "select_order_by_text",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)", "INSERT INTO users VALUES (1, 'charlie'), (2, 'alice'), (3, 'bob')"},
		MainSQL:  "SELECT id, name FROM users ORDER BY name ASC",
		Tags:     []string{"dml", "select", "order-by"},
	},
	{
		Name:     "drop_table_if_exists",
		Kind:     KindExec,
		SetupSQL: []string{},
		MainSQL:  "DROP TABLE IF EXISTS nonexistent",
		Tags:     []string{"ddl", "drop"},
	},
	{
		Name:     "select_empty_table",
		Kind:     KindQuery,
		SetupSQL: []string{"CREATE TABLE users (id int, name text)"},
		MainSQL:  "SELECT * FROM users",
		Tags:     []string{"dml", "select"},
	},
	{
		Name:     "insert_multiple_then_count",
		Kind:     KindQuery,
		SetupSQL: []string{
			"CREATE TABLE users (id int, name text)",
			"INSERT INTO users VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
		},
		MainSQL: "SELECT id FROM users ORDER BY id LIMIT 3 OFFSET 1",
		Tags:    []string{"dml", "select", "limit", "offset", "order-by"},
	},
}

func getTestConn(t *testing.T) *pgx.Conn {
	t.Helper()
	host := os.Getenv("FAKEGRES_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("FAKEGRES_PORT")
	if port == "" {
		port = "6000"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	connStr := fmt.Sprintf("postgres://%s:%s/postgres?sslmode=disable", host, port)
	config, err := pgx.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("failed to parse config: %v", err)
	}
	// Use simple protocol to avoid prepared statements
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	return conn
}

func resetDatabase(t *testing.T, conn *pgx.Conn) {
	// We use a fresh connection per test; the server should be reset between test runs
}

func executeSQL(ctx context.Context, conn *pgx.Conn, sql string) ([][]string, []string, error) {
	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var colNames []string
	for _, fd := range rows.FieldDescriptions() {
		colNames = append(colNames, string(fd.Name))
	}

	var results [][]string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, nil, err
		}
		row := make([]string, len(vals))
		for i, v := range vals {
			row[i] = fmt.Sprintf("%v", v)
		}
		results = append(results, row)
	}

	return results, colNames, rows.Err()
}

func execSQL(ctx context.Context, conn *pgx.Conn, sql string) error {
	_, err := conn.Exec(ctx, sql)
	return err
}

func sortRows(rows [][]string) {
	sort.Slice(rows, func(i, j int) bool {
		for k := 0; k < len(rows[i]) && k < len(rows[j]); k++ {
			if rows[i][k] != rows[j][k] {
				return rows[i][k] < rows[j][k]
			}
		}
		return len(rows[i]) < len(rows[j])
	})
}

func TestCompatibility(t *testing.T) {
	passed := 0
	failed := 0
	skipped := 0

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.Skip != "" {
				t.Skipf("Skipping: %s", tc.Skip)
				skipped++
				return
			}

			conn := getTestConn(t)
			defer conn.Close(context.Background())

			ctx := context.Background()

			// Run setup
			for _, sql := range tc.SetupSQL {
				if err := execSQL(ctx, conn, sql); err != nil {
					t.Fatalf("Setup failed for %q: %v", sql, err)
				}
			}

			// Run main SQL
			switch tc.Kind {
			case KindExec:
				if err := execSQL(ctx, conn, tc.MainSQL); err != nil {
					t.Errorf("Exec failed: %v", err)
					failed++
					return
				}
			case KindQuery:
				rows, cols, err := executeSQL(ctx, conn, tc.MainSQL)
				if err != nil {
					t.Errorf("Query failed: %v", err)
					failed++
					return
				}
				t.Logf("Columns: %v", cols)
				t.Logf("Rows: %v", rows)
			}
			passed++
		})
	}

	t.Logf("\n=== SUMMARY ===")
	t.Logf("Passed: %d", passed)
	t.Logf("Failed: %d", failed)
	t.Logf("Skipped: %d", skipped)
	t.Logf("Total: %d", len(testCases))
}

func TestSummary(t *testing.T) {
	tagCounts := make(map[string]int)
	tagSkipped := make(map[string]int)

	for _, tc := range testCases {
		for _, tag := range tc.Tags {
			tagCounts[tag]++
			if tc.Skip != "" {
				tagSkipped[tag]++
			}
		}
	}

	t.Logf("\n=== TEST COVERAGE BY TAG ===")
	var tags []string
	for tag := range tagCounts {
		tags = append(tags, tag)
	}
	sort.Strings(tags)

	for _, tag := range tags {
		total := tagCounts[tag]
		skipped := tagSkipped[tag]
		implemented := total - skipped
		t.Logf("%-15s: %d/%d implemented", tag, implemented, total)
	}
}

func TestListNotImplemented(t *testing.T) {
	t.Logf("\n=== NOT IMPLEMENTED FEATURES ===")
	reasons := make(map[string][]string)
	for _, tc := range testCases {
		if tc.Skip != "" {
			reasons[tc.Skip] = append(reasons[tc.Skip], tc.Name)
		}
	}

	var skipReasons []string
	for reason := range reasons {
		skipReasons = append(skipReasons, reason)
	}
	sort.Strings(skipReasons)

	for _, reason := range skipReasons {
		t.Logf("\n%s:", reason)
		for _, name := range reasons[reason] {
			t.Logf("  - %s", name)
		}
	}
}

// Individual feature tests for tracking progress
func TestFeature_CreateTable(t *testing.T) {
	runTestsWithTag(t, "create")
}

func TestFeature_Insert(t *testing.T) {
	runTestsWithTag(t, "insert")
}

func TestFeature_Select(t *testing.T) {
	runTestsWithTag(t, "select")
}

func TestFeature_Delete(t *testing.T) {
	runTestsWithTag(t, "delete")
}

func TestFeature_Where(t *testing.T) {
	runTestsWithTag(t, "where")
}

func TestFeature_Update(t *testing.T) {
	runTestsWithTag(t, "update")
}

func runTestsWithTag(t *testing.T, tag string) {
	for _, tc := range testCases {
		hasTag := false
		for _, tg := range tc.Tags {
			if tg == tag {
				hasTag = true
				break
			}
		}
		if !hasTag {
			continue
		}

		t.Run(tc.Name, func(t *testing.T) {
			if tc.Skip != "" {
				t.Skipf("Skipping: %s", tc.Skip)
				return
			}

			conn := getTestConn(t)
			defer conn.Close(context.Background())

			ctx := context.Background()

			for _, sql := range tc.SetupSQL {
				if err := execSQL(ctx, conn, sql); err != nil {
					if !strings.Contains(err.Error(), "already exists") {
						t.Fatalf("Setup failed for %q: %v", sql, err)
					}
				}
			}

			switch tc.Kind {
			case KindExec:
				if err := execSQL(ctx, conn, tc.MainSQL); err != nil {
					t.Errorf("Exec failed: %v", err)
					return
				}
			case KindQuery:
				rows, cols, err := executeSQL(ctx, conn, tc.MainSQL)
				if err != nil {
					t.Errorf("Query failed: %v", err)
					return
				}
				t.Logf("Columns: %v, Rows: %d", cols, len(rows))
			}
		})
	}
}
