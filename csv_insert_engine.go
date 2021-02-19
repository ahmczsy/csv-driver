package csvdriver

import (
	"database/sql/driver"
	"encoding/csv"
	"errors"
	"io"
	"os"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

func insertRows(fi *os.File, parser *sqlparser.Insert, args []driver.Value) error {
	row := parser.Rows.(sqlparser.Values)
	rowVal, err := getInsertRows(row, args)
	if err != nil {
		return err
	}
	cols, err := getInsertCol(fi, parser)
	if err != nil {
		return err
	}
	actualInsertRow := []string{}
	for i := range cols {
		if len(rowVal)-1 < i {
			actualInsertRow = append(actualInsertRow, "<null>")
		} else {
			actualInsertRow = append(actualInsertRow, rowVal[i])
		}
	}
	return writeCSV(fi, actualInsertRow)
}

// 获取要插入的数据
func getInsertRows(rows sqlparser.Values, args []driver.Value) ([]string, error) {
	rowVals := []string{}
	for _, tupleRows := range rows {
		for _, item := range tupleRows {
			val := item.(*sqlparser.SQLVal)
			strVal, err := parseArg(val, args)
			if err != nil {
				return nil, err
			}
			rowVals = append(rowVals, strVal)
		}
	}
	return rowVals, nil
}

// 获取要插入的列
func getInsertCol(fi *os.File, parser *sqlparser.Insert) ([]string, error) {
	cols := []string{}
	for _, col := range parser.Columns {
		cols = append(cols, col.String())
	}
	if len(cols) != 0 {
		return cols, nil
	}

	_, err := fi.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	reader := csv.NewReader(fi)
	header, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, errors.New(`table not exists`)
		} else {
			return nil, err
		}
	}
	cols = append(cols, header...)
	if len(cols) == 0 {
		return nil, errors.New("header len is 0")
	}
	return cols, nil
}
