# CSV-Driver 

Golang SQL database driver for CSV

In order to learn the source code of the goalng database driver , I implemented the CSV driver

## Supported query

* create table
* insert row
* simple query


## Install
```
go get -u github.com/ahmczsy/csv-driver
```

## Example

For more examples, please refer to the [examples](https://github.com/ahmczsy/csv-driver/tree/main/examples)

```go
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
```
