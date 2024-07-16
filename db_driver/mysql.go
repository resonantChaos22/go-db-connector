package db_driver

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type MySQL struct {
	DSN string
	db  *sql.DB
}

func (s *MySQL) openDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (s *MySQL) ConnectToDB() error {
	// dsn := os.Getenv("DSN")
	dsn := s.DSN
	for {
		connection, err := s.openDB(dsn)
		if err != nil {
			log.Println("MySQL not yet ready...")
			log.Println(err)
			counts++
			if counts > 10 {
				log.Println(err)
				return err
			}
			log.Println("Pausing for 2 seconds...")
			time.Sleep(2 * time.Second)
			continue
		}
		log.Println("Connected to Mysq!")
		s.db = connection
		return nil
	}
}

func (s *MySQL) GetDB() *sql.DB {
	return s.db
}

func (s *MySQL) ListDatabases() ([]string, error) {
	query := `
	SELECT schema_name
	FROM information_schema.schemata
	WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
`
	rows, err := s.db.Query(query)
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

func (s *MySQL) GetTables(schema string) ([]Table, error) {
	query := `
	SELECT table_name
	FROM information_schema.tables
	WHERE table_schema = ?
`
	rows, err := s.db.Query(query, schema)
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

		columns, err := s.getColumns(schema, tableName)
		if err != nil {
			return nil, err
		}
		table.Columns = append(table.Columns, columns...)

		tables = append(tables, table)
	}
	return tables, nil
}

func (s *MySQL) GetTablesFromDatabase(database, schema string) ([]Table, error) {
	return s.GetTables(database)
}

func (s *MySQL) getColumns(database, tableName string) ([]ColumnInfo, error) {
	query := `
	SELECT column_name, data_type
	FROM information_schema.columns
	WHERE table_name = ?
`
	rows, err := s.db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pKeys, err := s.getPrimaryKeys(database, tableName)
	if err != nil {
		return nil, err
	}

	var columns []ColumnInfo
	for rows.Next() {
		var column ColumnInfo
		if err := rows.Scan(&column.Name, &column.Type); err != nil {
			return nil, err
		}

		if column.Name == pKeys[0] {
			column.IsPrimary = true
		}

		columns = append(columns, column)
	}
	return columns, nil
}

func (s *MySQL) getPrimaryKeys(database, tableName string) ([]string, error) {
	query := `
		SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY'
	`
	rows, err := s.db.Query(query, database, tableName)
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
	log.Println("HELO ", tableName)
	log.Println(primaryKeys)
	return primaryKeys, nil
}
