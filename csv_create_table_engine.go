package csvdriver

import (
	"database/sql/driver"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

func createTable(fi *os.File, create *sqlparser.CreateTable) error {
	_, err := csv.NewReader(fi).Read()
	switch err {
	case nil:
		return errors.New("table is exists")
	case io.EOF:
		cols := getCreateCol(create)
		return writeCSV(fi, cols)
	default:
		return err
	}
}

func getCreateCol(parser *sqlparser.CreateTable) []string {
	cols := []string{}
	for _, temp := range parser.Columns {
		item := temp
		cols = append(cols, item.Name)
	}
	return cols
}


func writeCSV(fi *os.File, data []string) error {
	if _, err := fi.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	writer := csv.NewWriter(fi)
	if err := writer.Write(data); err != nil {
		return err
	}
	writer.Flush()
	return writer.Error()
}

func parseArg(val *sqlparser.SQLVal, args []driver.Value) (string, error) {
	strVal := ""
	if val.Type == sqlparser.ValArg {
		idx, err := parserArgIndex(string(val.Val))
		if err != nil {
			return "", err
		}
		if idx > len(args) {
			return "", errors.New(`wrong number of args`)
		} else {
			strVal = fmt.Sprint(args[idx-1])
		}
	} else {
		strVal = string(val.Val)
	}
	return strVal, nil
}

func parserArgIndex(args string) (int, error) {
	numberStr := strings.ReplaceAll(args, `:v`, ``)
	idx, err := strconv.ParseInt(numberStr, 10, 64)
	return int(idx), err
}


