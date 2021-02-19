package main

import (
	"database/sql"
	"fmt"

	csvdriver "csv-driver"
)

func main() {
	conn, err := sql.Open(csvdriver.DriverName, "./test.csv")
	if err != nil {
		panic(err)
	}
	rows, err := conn.Query(`select * from xxx where a > 'a'`)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var a, b, c string
		err := rows.Scan(&a, &b, &c)
		if err != nil {
			panic(err)
		}
		fmt.Println(a, b, c)
	}
}
