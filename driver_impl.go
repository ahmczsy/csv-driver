package csvdriver

import (
	"database/sql/driver"
	"errors"
	"io"
	"os"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

type csvConn struct {
	fi *os.File
}

func (c *csvConn) Prepare(query string) (driver.Stmt, error) {
	panic("unsupport prepare")
}

func (c *csvConn) Close() error {
	return c.fi.Close()
}

func (c *csvConn) Begin() (driver.Tx, error) {
	panic("unsupport begin")
}

func (c *csvConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	parser, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	selectParser, ok := parser.(*sqlparser.Select)
	if !ok {
		return nil, errors.New("not select sql")
	}
	comparisonExpr, ok := selectParser.Where.Expr.(*sqlparser.ComparisonExpr)
	if !ok {
		return nil, errors.New("only support simple comparison exper ,such as : where  a = 'aaa'")
	}
	cols, datas, err := queryCSV(c.fi, comparisonExpr, selectParser.SelectExprs, args)
	if err != nil {
		return nil, err
	}
	return &csvRows{cols: cols, datas: datas}, nil
}

func (c *csvConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	parser, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}
	createParser, isCreate := parser.(*sqlparser.CreateTable)
	insertParser, isInsert := parser.(*sqlparser.Insert)
	if !(isInsert || isCreate) {
		return nil, errors.New("not create_table or insert sql")
	}
	if isCreate {
		return nil, createTable(c.fi, createParser)
	} else {
		return nil, insertRows(c.fi, insertParser, args)
	}
}

type csvRows struct {
	// 查询的列
	cols []string
	// 所有符合 where 的数据
	datas [][]string
	// next 的游标
	index int
}

func (r *csvRows) Columns() []string {
	return r.cols
}

func (r *csvRows) Close() error {
	return nil
}

func (r *csvRows) Next(dest []driver.Value) error {
	if len(r.datas)-1 < r.index {
		return io.EOF
	}
	for i := range dest {
		dest[i] = r.datas[r.index][i]
	}
	r.index++
	return nil
}
