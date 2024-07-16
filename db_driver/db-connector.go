package db_driver

import (
	"database/sql"
	"log"
)

type DBInterface interface {
	ConnectToDB() error
	GetDB() *sql.DB
	ListDatabases() ([]string, error)
	GetTables(schema string) ([]Table, error)
	GetTablesFromDatabase(database, schema string) ([]Table, error)
}

type ColumnInfo struct {
	Name      string
	Type      string
	IsPrimary bool
}

type Table struct {
	Name    string
	Columns []ColumnInfo
}

func CreateNewDBInstance(db string) DBInterface {
	log.Println(db)
	switch db {
	case "postgres":
		return &PostgresDB{
			DSN: "host=127.0.0.1 port=5432 user=postgres password=password dbname=postgres sslmode=disable timezone=UTC connect_timeout=5",
		}
	case "mysql":
		return &MySQL{
			DSN: "root:password@tcp(127.0.0.1:3306)/users",
		}
	default:
		return &PostgresDB{
			DSN: "host=127.0.0.1 port=5432 user=postgres password=password dbname=postgres sslmode=disable timezone=UTC connect_timeout=5",
		}
	}
}
