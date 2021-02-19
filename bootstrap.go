package csvdriver

import (
	"database/sql"
	"database/sql/driver"
	"os"
)

const DriverName = `csv`

func init() {
	sql.Register(DriverName, &csvDrive{})
}

type csvDrive struct{}

func (d *csvDrive) Open(name string) (driver.Conn, error) {
	fi, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &csvConn{fi: fi}, nil
}
