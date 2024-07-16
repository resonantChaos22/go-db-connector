package db_driver

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/jackc/pgconn"
	_ "github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	counts int = 0
)

type PostgresDB struct {
	DSN string
	db  *sql.DB
}

func (p *PostgresDB) openDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (p *PostgresDB) ConnectToDB() error {
	// dsn := os.Getenv("DSN")
	dsn := p.DSN
	for {
		connection, err := p.openDB(dsn)
		if err != nil {
			log.Println("Postgres not yet ready...")
			counts++
			if counts > 10 {
				log.Println(err)
				return err
			}
			log.Println("Pausing for 2 seconds...")
			time.Sleep(2 * time.Second)
			continue
		}
		// log.Println("Connected to Postgres!")
		p.db = connection
		return nil
	}
}

func (p *PostgresDB) GetDB() *sql.DB {
	return p.db
}

func (p *PostgresDB) ListDatabases() ([]string, error) {
	query := `
		SELECT datname
		FROM pg_database
		WHERE datistemplate = false
	`
	rows, err := p.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, err
		}
		databases = append(databases, dbName)
	}
	return databases, nil
}

func (p *PostgresDB) GetTables(schema string) ([]Table, error) {
	query := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = $1
	`
	rows, err := p.db.Query(query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []Table
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		table := Table{
			Name:    tableName,
			Columns: []ColumnInfo{},
		}

		columns, err := p.getColumns(tableName)
		if err != nil {
			log.Printf("Err in getting columns: %+v", err)
			return nil, err
		}

		table.Columns = append(table.Columns, columns...)

		tables = append(tables, table)
	}

	return tables, nil
}

func (p *PostgresDB) GetTablesFromDatabase(database, schema string) ([]Table, error) {
	if schema == "" {
		schema = "public"
	}
	// Save the current DSN
	originalDSN := p.DSN
	defer func() {
		p.DSN = originalDSN
		p.ConnectToDB()
	}()

	// Create a new DSN for the target database
	newDSN := fmt.Sprintf("host=localhost port=5432 user=postgres password=password dbname=%s sslmode=disable timezone=UTC connect_timeout=5", database)

	// Connect to the new database
	p.DSN = newDSN
	p.ConnectToDB()

	// Get the tables from the specified schema
	tables, err := p.GetTables(schema)
	if err != nil {
		return nil, err
	}

	return tables, nil
}

func (p *PostgresDB) getPrimaryKeys(table string) ([]string, error) {
	query := `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		ON tc.constraint_name = kcu.constraint_name
		WHERE tc.table_name = $1 AND tc.constraint_type = 'PRIMARY KEY'
	`
	rows, err := p.db.Query(query, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		primaryKeys = append(primaryKeys, columnName)
	}
	return primaryKeys, nil
}

func (p *PostgresDB) getColumns(table string) ([]ColumnInfo, error) {

	primaryKeys, err := p.getPrimaryKeys(table)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_name = $1
	`
	rows, err := p.db.Query(query, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var column ColumnInfo
		if err := rows.Scan(&column.Name, &column.Type); err != nil {
			return nil, err
		}
		if column.Name == primaryKeys[0] {
			column.IsPrimary = true
		}
		columns = append(columns, column)
	}
	return columns, nil
}
