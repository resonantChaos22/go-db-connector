package main

import (
	"fmt"
	"log"
	"resonantchaos22/go-db-connector/db_driver"
	"strings"
)

func main() {
	fmt.Println("Hello Main")

	dbDriver := db_driver.CreateNewDBInstance("mysql")
	if dbDriver != nil {
		err := dbDriver.ConnectToDB()
		if err != nil {
			panic("Cant connect To DB")
		}
		db := dbDriver.GetDB()
		if db != nil {
			databases, err := dbDriver.ListDatabases()
			if err != nil {
				panic(err)
			}

			for _, database := range databases {
				log.Println(strings.ToUpper(database))
				tables, err := dbDriver.GetTablesFromDatabase(database, "public")
				if err != nil {
					log.Printf("%s error\n", database)
					log.Println(err)
				}

				for _, table := range tables {
					log.Printf("%+v", table)
				}
			}
		}
	}
}
