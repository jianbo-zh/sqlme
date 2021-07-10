package sqlme

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

func NewQueryBuilder(table string, options ...QueryOption) *QueryBuilder {

	query := &QueryBuilder{
		Table: table,
	}
	for _, option := range options {
		option(query)
	}
	return query
}

func (qb *QueryBuilder) BuildQuery(options ...QueryOption) (qSql string, qVals []interface{}, err error) {

	for _, option := range options {
		option(qb)
	}

	if qb.Table == "" {
		err = errors.New("table name can not empty")
		return
	} else if len(qb.Columns) == 0 {
		err = errors.New("column must appoint")
		return
	}

	qSql = "select " + qb.BuildDistinct() + strings.Join(qb.Columns, ",") + " from " + qb.Table

	// build where snippet
	wSql, wVals, wErr := qb.BuildWheres(qb.Wheres...)
	if wErr != nil {
		err = errors.Wrap(err, "BuildWheres faild")
		return
	}

	qSql += " " + wSql
	qVals = append(qVals, wVals...)

	// build orderby snippet
	oSql := qb.BuildOrderBy()
	if oSql != "" {
		qSql += " " + oSql
	}

	// build limit snippet
	lSql, lVals := qb.BuildLimit()
	if lSql != "" {
		qSql += " " + lSql
		qVals = append(qVals, lVals...)
	}

	return
}

func (qb *QueryBuilder) BuildDelete() (qSql string, qVals []interface{}, err error) {
	if qb.Table == "" {
		err = errors.New("table name can not empty")
		return
	}

	qSql = "delete from " + qb.Table

	// build where
	wSql, wVals, wErr := qb.BuildWheres(qb.Wheres...)
	if wErr != nil {
		err = errors.Wrap(wErr, "buildWheres faild")
	}
	if wSql != "" {
		qSql += " " + wSql
		qVals = append(qVals, wVals...)
	}

	// build orderby snippet
	oSql := qb.BuildOrderBy()
	if oSql != "" {
		qSql += " " + oSql
	}

	// build limit snippet
	lSql, lVals := qb.BuildLimit()
	if lSql != "" {
		qSql += " " + lSql
		qVals = append(qVals, lVals...)
	}

	return
}

// BuildCreate
func (qb *QueryBuilder) BuildCreate() (qSql string, qVals []interface{}, err error) {
	if qb.Table == "" {
		err = errors.New("table name can not empty")
		return
	}

	qSql = "insert into " + qb.Table

	if len(qb.Datas) == 0 {
		err = errors.New("insert data is empty")
		return
	}

	columns := []string{}
	placeholders := []string{}
	values := []interface{}{}

	for col, val := range qb.Datas {
		columns = append(columns, col)
		values = append(values, val)
		placeholders = append(placeholders, "?")
	}

	qSql += "(" + strings.Join(columns, ",") + ") values (" + strings.Join(placeholders, ",") + ")"
	qVals = append(qVals, values...)

	return
}

// BuildUpdate
func (qb *QueryBuilder) BuildUpdate() (qSql string, qVals []interface{}, err error) {
	if qb.Table == "" {
		err = errors.New("table name can not empty")
		return
	}

	qSql = "update " + qb.Table

	// build update data
	uSql, uVals, uErr := qb.BuildSetData()
	if uErr != nil {
		err = errors.Wrap(uErr, "BuildSetData failed")
		return
	}

	qSql += " set " + uSql
	qVals = append(qVals, uVals...)

	// build where
	wSql, wVals, wErr := qb.BuildWheres(qb.Wheres...)
	if wErr != nil {
		err = errors.Wrap(wErr, "buildWheres faild")
	}
	if wSql != "" {
		qSql += " " + wSql
		qVals = append(qVals, wVals...)
	}

	// build orderby snippet
	oSql := qb.BuildOrderBy()
	if oSql != "" {
		qSql += " " + oSql
	}

	// build limit snippet
	lSql, lVals := qb.BuildLimit()
	if lSql != "" {
		qSql += " " + lSql
		qVals = append(qVals, lVals...)
	}

	return
}

// BuildWheres
func (qb *QueryBuilder) BuildWheres(wheres ...Where) (wSql string, wVals []interface{}, err error) {

	for index, where := range wheres {

		if index == 0 {
			wSql += " where "
		} else {
			andOr := AndOr_And
			if where.AndOr != "" {
				andOr = where.AndOr
			}
			wSql += " " + andOr + " "
		}

		switch where.Type {
		case Type_Sub:
			subWheres, ok := where.Value.([]Where)
			if !ok {
				err = errors.New("subtype value need []Where")
				return
			}
			sSql, sVals, sErr := qb.BuildWheres(subWheres...)
			if sErr != nil {
				err = sErr
				return
			}
			wSql += "(" + sSql + ")"
			wVals = append(wVals, sVals...)
		default:
			dSql, dVals, dErr := qb.buildWhere(where)
			if dErr != nil {
				err = dErr
				return
			}
			wSql += dSql
			wVals = append(wVals, dVals...)
		}
	}

	return
}

// buildWhere
func (qb *QueryBuilder) buildWhere(where Where) (wSql string, wVals []interface{}, err error) {

	switch where.Operator {
	case Oper_Egt:
		wSql = where.Column + " >= ?"
		wVals = append(wVals, where.Value)
	case Oper_Gt:
		wSql = where.Column + " > ?"
		wVals = append(wVals, where.Value)
	case Oper_Et:
		wSql = where.Column + " = ?"
		wVals = append(wVals, where.Value)
	case Oper_NotEt:
		wSql = where.Column + " != ?"
		wVals = append(wVals, where.Value)
	case Oper_Lt:
		wSql = where.Column + " < ?"
		wVals = append(wVals, where.Value)
	case Oper_Elt:
		wSql = where.Column + " <= ?"
		wVals = append(wVals, where.Value)
	case Oper_Like:
		wSql = where.Column + " like ?"
		wVals = append(wVals, where.Value)
	case Oper_NotLike:
		wSql = where.Column + " not like ?"
		wVals = append(wVals, where.Value)
	case Oper_In:
		values, ok := where.Value.([]interface{})
		if !ok {
			err = errors.New("where in need slice args")
			return
		}
		plArr := []string{}
		for range values {
			plArr = append(plArr, "?")
		}
		wSql = where.Column + " in (" + strings.Join(plArr, ",") + ")"
		wVals = append(wVals, values...)
	case Oper_NotIn:
		values, ok := where.Value.([]interface{})
		if !ok {
			err = errors.New("where not in need slice args")
			return
		}
		plArr := []string{}
		for range values {
			plArr = append(plArr, "?")
		}
		wSql = where.Column + " not in (" + strings.Join(plArr, ",") + ")"
		wVals = append(wVals, values...)
	case Oper_Between:
		values := where.Value.([]interface{})
		if len(values) != 2 {
			err = errors.New("where between need slice args and only 2 item")
			return
		}
		wSql = where.Column + " between ? and ? "
		wVals = append(wVals, values...)
	default:
		err = errors.New(fmt.Sprintf("where %s unsurpport", where.Operator))
		return
	}
	return
}

// BuildDistinct
func (qb *QueryBuilder) BuildDistinct() (distinct string) {
	if qb.Distinct {
		distinct = " distinct "
	}

	return
}

// BuildLimit
func (qb *QueryBuilder) BuildLimit() (lSql string, lVals []interface{}) {
	if len(qb.Limits) == 0 {
		return
	}

	if len(qb.Limits) == 1 {
		lSql = "limit ?"
		lVals = append(lVals, qb.Limits[0])
	} else if len(qb.Limits) >= 2 {
		lSql = "limit ?, ?"
		lVals = append(lVals, qb.Limits[0], qb.Limits[1])
	}
	return
}

// BuildOrderBy
func (qb *QueryBuilder) BuildOrderBy() (orderBy string) {
	if len(qb.OrderBys) != 0 {
		orderBy = " order by " + strings.Join(qb.OrderBys, ",")
	}

	return
}

// BuildSetData
func (qb *QueryBuilder) BuildSetData() (uSql string, uVals []interface{}, err error) {
	if len(qb.Datas) == 0 {
		err = errors.New("datas can not empty")
		return
	}

	i := 0
	for col, val := range qb.Datas {
		if i == 0 {
			uSql += col + " = ?"
		} else {
			uSql += ", " + col + " = ?"
		}
		uVals = append(uVals, val)
		i++
	}

	return
}
