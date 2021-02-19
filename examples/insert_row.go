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
	_, err = conn.Exec(` insert into xxx values ('9456n','sdfs','q23234')`)
	if err != nil {
		panic(err)
	}
}
