package main

import (
	"database/sql"

	"github.com/ahmczsy/csv-driver"
)

func main() {
	conn, err := sql.Open(csvdriver.DriverName, "./test.csv")
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec(`create table xxx (a varchar(255),b varchar(255),c varchar(255))`)
	if err != nil {
		panic(err)
	}
}
