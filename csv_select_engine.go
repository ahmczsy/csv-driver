package csvdriver

import (
	"database/sql/driver"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

func queryCSV(fi *os.File, whereExpr *sqlparser.ComparisonExpr, selectExprs sqlparser.SelectExprs, args []driver.Value) ([]string, [][]string, error) {
	whereCol, ok := whereExpr.Left.(*sqlparser.ColName)
	if !ok {
		return nil, nil, errors.New("等式的左侧是必须是列名")
	}
	whereColName := whereCol.Name.String()

	header, err := readerHeader(fi)
	if err != nil {
		if err == io.EOF {
			return nil, nil, errors.New(`table not exists`)
		} else {
			return nil, nil, err
		}
	}
	aliasColArr, selectColArr, _ := getSelectCols(selectExprs, header)

	whereIdx := -1
	selectColIndexArr := []int{}
	for i, colItem := range header {
		if colItem == whereColName {
			whereIdx = i
		}
		for j, selectColItem := range selectColArr {
			if colItem == selectColItem {
				selectColIndexArr = append(selectColIndexArr, j)
			}
		}
	}

	if len(selectColArr) != len(selectColIndexArr) {
		return nil, nil, errors.New("查询的部分列，没有在 csv 中找到")
	}
	if whereIdx == -1 {
		return nil, nil, errors.New(fmt.Sprintf("没有找到「%s」列", whereColName))
	}
	sqlVal, ok := whereExpr.Right.(*sqlparser.SQLVal)
	if !ok {
		return nil, nil, errors.New("等式的左侧是必须是值")
	}
	strVal, err := parseArg(sqlVal, args)
	if err != nil {
		return nil, nil, err
	}

	allDatas, err := readerAllData(fi)
	if err != nil {
		if err == io.EOF {
			return aliasColArr, nil, nil
		} else {
			return nil, nil, err
		}
	}
	resultData, err := filterData(allDatas, whereExpr.Operator, whereIdx, selectColIndexArr, strVal)
	if err != nil {
		return nil, nil, err
	}
	return aliasColArr, resultData, nil

}

func readerHeader(fi *os.File) ([]string, error) {
	if _, err := fi.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	reader := csv.NewReader(fi)
	return reader.Read()
}

func readerAllData(fi *os.File) ([][]string, error) {
	if _, err := fi.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	reader := csv.NewReader(fi)
	if _, err := reader.Read(); err != nil {
		return nil, err
	}
	return reader.ReadAll()
}

func filterData(datas [][]string, operator string, whereIdx int, selectColIndexArr []int, whereVal string) ([][]string, error) {
	result := [][]string{}
	for _, row := range datas {
		csvCell := row[whereIdx]
		flag := false
		switch operator {
		case sqlparser.EqualStr:
			flag = csvCell == whereVal
		case sqlparser.LessThanStr:
			flag = csvCell < whereVal
		case sqlparser.GreaterThanStr:
			flag = csvCell > whereVal
		case sqlparser.LessEqualStr:
			flag = csvCell <= whereVal
		case sqlparser.GreaterEqualStr:
			flag = csvCell >= whereVal
		case sqlparser.NotEqualStr:
			flag = csvCell != whereVal
		default:
			return nil, errors.New("不支持的 where 条件")
		}
		if !flag {
			continue
		}
		reusltRow := []string{}
		for i := range selectColIndexArr {
			reusltRow = append(reusltRow, row[i])
		}
		result = append(result, reusltRow)
	}
	return result, nil
}

func getSelectCols(selectExprs sqlparser.SelectExprs, header []string) ([]string, []string, error) {
	aliasCols := []string{}
	selectCols := []string{}
	for _, item := range selectExprs {
		aliasedExpr, aliasOK := item.(*sqlparser.AliasedExpr)
		if !aliasOK {
			_, starOK := item.(*sqlparser.StarExpr)
			if starOK {
				a1 := make([]string, len(header), len(header))
				a2 := make([]string, len(header), len(header))
				copy(a1, header)
				copy(a2, header)
				return a1, a2, nil
			} else {
				buffer := sqlparser.NewTrackedBuffer(nil)
				selectExprs.Format(buffer)
				return nil, nil, errors.New(fmt.Sprintf("unsupport 「%s」select expression", buffer.String()))
			}
		}
		selectColName := aliasedExpr.Expr.(*sqlparser.ColName)
		col := selectColName.Name.String()
		if aliasedExpr.As.String() == "" {
			aliasCols = append(aliasCols, col)
		} else {
			aliasCols = append(aliasCols, aliasedExpr.As.String())
		}
		selectCols = append(selectCols, col)
	}
	return aliasCols, selectCols, nil
}
