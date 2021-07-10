package sqlme

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

func NewDB(drv, dsn string, options ...MyDBOption) (*MyDB, error) {
	var db *MyDB
	sqldb, err := sql.Open(drv, dsn)
	if err != nil {
		return db, errors.Wrap(err, fmt.Sprintf("sql.Open failed: %s, %s", drv, dsn))
	}
	db = &MyDB{
		DB:           sqldb,
		NoRowRtnZero: true,
	}
	for _, option := range options {
		option(db)
	}

	return db, nil
}

func NewDB2(sqldb *sql.DB, options ...MyDBOption) *MyDB {
	db := &MyDB{
		DB:           sqldb,
		NoRowRtnZero: true,
	}
	for _, option := range options {
		option(db)
	}

	return db
}

// Insert
func (db *MyDB) Insert(table string, datas Data) (int64, error) {
	var iid int64
	var err error

	query := &QueryBuilder{
		Table: table,
		Datas: datas,
	}

	sqlStr, vals, err := query.BuildCreate()
	if err != nil {
		return iid, errors.Wrap(err, "BuildCreate failed")
	}

	result, err := db.DB.Exec(sqlStr, vals...)
	if err != nil {
		return iid, err
	}

	iid, err = result.LastInsertId()
	if err != nil {
		return iid, err
	}

	return iid, nil
}

func (db *MyDB) InsertBatch(table string, datas []Data) (int64, error) {
	var inum int64
	var err error

	columns := []string{}
	placeholders := []string{}

	if len(datas) == 0 {
		return inum, errors.New("data is empty")
	}

	for key := range datas[0] {
		columns = append(columns, key)
		placeholders = append(placeholders, "?")
	}

	sqlStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(columns, ","), strings.Join(placeholders, ","))

	stmt, err := db.DB.Prepare(sqlStr)
	if err != nil {
		return inum, errors.Wrap(err, "Prepare sql failed")
	}

	for index, data := range datas {
		var values []interface{}
		for _, field := range columns {
			if value, ok := data[field]; !ok {
				return inum, errors.New(fmt.Sprintf("datas[%d].%s not exists", index, field))
			} else {
				values = append(values, value)
			}
		}

		_, err := stmt.Exec(values...)
		if err != nil {
			return inum, errors.Wrap(err, "stmt.Exec failed")
		}

		inum++
	}

	return inum, nil
}

func (db *MyDB) InsertRaw(sqlStr string, vals ...interface{}) (int64, error) {
	var iid int64

	res, err := db.DB.Exec(sqlStr, vals...)
	if err != nil {
		return iid, errors.Wrap(err, "db.Exec failed")
	}

	iid, nil := res.LastInsertId()
	if err != nil {
		return iid, errors.Wrap(err, "result.LastInsertId failed")
	}

	return iid, nil
}

// Delete
func (db *MyDB) Delete(table string, wheres ...Where) (int64, error) {
	var dnum int64

	query := &QueryBuilder{
		Table:  table,
		Wheres: wheres,
	}

	sqlStr, vals, err := query.BuildDelete()
	if err != nil {
		return dnum, errors.Wrap(err, "BuildDelete failed")
	}
	res, err := db.DB.Exec(sqlStr, vals...)
	if err != nil {
		return dnum, errors.Wrap(err, "db.Exec failed")
	}

	dnum, nil := res.RowsAffected()
	if err != nil {
		return dnum, errors.Wrap(err, "result.RowsAffected failed")
	}

	return dnum, nil
}

// DeleteLimit
func (db *MyDB) DeleteLimit(table string, limit []int, wheres ...Where) (int64, error) {
	var dnum int64

	query := &QueryBuilder{
		Table:  table,
		Wheres: wheres,
		Limits: limit,
	}

	sqlStr, vals, err := query.BuildDelete()
	if err != nil {
		return dnum, errors.Wrap(err, "BuildDelete failed")
	}
	res, err := db.DB.Exec(sqlStr, vals...)
	if err != nil {
		return dnum, errors.Wrap(err, "db.Exec failed")
	}

	dnum, nil := res.RowsAffected()
	if err != nil {
		return dnum, errors.Wrap(err, "result.RowsAffected failed")
	}

	return dnum, nil
}

// DeleteRaw
func (db *MyDB) DeleteRaw(sqlStr string, vals ...interface{}) (int64, error) {
	var num int64

	res, err := db.DB.Exec(sqlStr, vals...)
	if err != nil {
		return num, errors.Wrap(err, "db.Exec failed")
	}

	num, nil := res.RowsAffected()
	if err != nil {
		return num, errors.Wrap(err, "result.RowsAffected failed")
	}

	return num, nil
}

// Update
func (db *MyDB) Update(table string, data Data, wheres ...Where) (int64, error) {
	var num int64
	query := &QueryBuilder{
		Table:  table,
		Wheres: wheres,
		Datas:  data,
	}

	sqlStr, vals, err := query.BuildUpdate()
	if err != nil {
		return num, errors.Wrap(err, "BuildUpdate faild")
	}

	res, err := db.DB.Exec(sqlStr, vals...)
	if err != nil {
		return num, errors.Wrap(err, "db.Exec failed")
	}

	num, nil := res.RowsAffected()
	if err != nil {
		return num, errors.Wrap(err, "result.RowsAffected failed")
	}

	return num, nil
}

func (db *MyDB) UpdateLimit(table string, data Data, limit []int, wheres ...Where) (int64, error) {
	var num int64

	query := &QueryBuilder{
		Table:  table,
		Wheres: wheres,
		Datas:  data,
		Limits: limit,
	}

	sqlStr, vals, err := query.BuildUpdate()
	if err != nil {
		return num, errors.Wrap(err, "BuildUpdate faild")
	}

	res, err := db.DB.Exec(sqlStr, vals...)
	if err != nil {
		return num, errors.Wrap(err, "db.Exec failed")
	}

	num, nil := res.RowsAffected()
	if err != nil {
		return num, errors.Wrap(err, "result.RowsAffected failed")
	}

	return num, nil
}

func (db *MyDB) UpdateRaw(sqlStr string, vals ...interface{}) (int64, error) {
	var num int64

	res, err := db.DB.Exec(sqlStr, vals...)
	if err != nil {
		return num, errors.Wrap(err, "db.Exec failed")
	}

	num, nil := res.RowsAffected()
	if err != nil {
		return num, errors.Wrap(err, "result.RowsAffected failed")
	}

	return num, nil
}

// Query
func (db *MyDB) Query(table string, columns []string, options ...QueryOption) ([]Row, error) {

	var rows []Row
	if len(columns) == 0 {
		return rows, errors.New("query columns can not empty")
	}

	query := NewQueryBuilder(table, options...)

	// 指定 columns
	sqlStr, vals, err := query.BuildQuery(ColumnQb(columns...))
	if err != nil {
		return rows, errors.Wrap(err, "query.BuildQuery failed")
	}

	result, err := db.DB.Query(sqlStr, vals...)
	if err != nil {
		return rows, errors.Wrap(err, "db.Query failed")
	}

	for result.Next() {
		row := make(Row, len(query.Columns))

		data := make([]interface{}, len(query.Columns))
		for idx := range data {
			data[idx] = &data[idx]
		}
		err := result.Scan(data...)
		if err != nil {
			return rows, errors.Wrap(err, "rows.Scan failed")
		}
		for idx, col := range query.Columns {
			row[col] = data[idx]
		}

		rows = append(rows, row)
	}

	return rows, nil
}

// QueryRow
func (db *MyDB) QueryRow(table string, columns []string, options ...QueryOption) (Row, error) {
	var row Row

	if len(columns) == 0 {
		return row, errors.New("query columns can not empty")
	}
	query := NewQueryBuilder(table, options...)

	// 指定 columns，强制 limit 1
	sqlStr, vals, err := query.BuildQuery(ColumnQb(columns...), LimitQb(0, 1))
	if err != nil {
		return row, errors.Wrap(err, "query.BuildQuery failed")
	}

	data := make([]interface{}, len(query.Columns))
	for idx := range data {
		data[idx] = &data[idx]
	}

	err = db.DB.QueryRow(sqlStr, vals...).Scan(data...)
	if err != nil {
		if err == sql.ErrNoRows && db.NoRowRtnZero {
			return row, nil
		}
		return row, errors.Wrap(err, "rows.Scan failed")
	}

	row = make(Row, len(query.Columns))
	for idx, col := range query.Columns {
		row[col] = data[idx]
	}

	return row, nil
}

func (db *MyDB) QueryRaw(sqlStr string, vals ...interface{}) ([]Row, error) {

	var rows []Row

	columns, err := parseSqlColumns(sqlStr)
	if err != nil {
		return rows, errors.Wrapf(err, "parseSqlColumns failed: %s", sqlStr)
	}

	result, err := db.DB.Query(sqlStr, vals...)
	if err != nil {
		return rows, errors.Wrap(err, "db.Query failed")
	}

	for result.Next() {
		row := make(Row, len(columns))

		data := make([]interface{}, len(columns))
		for idx := range data {
			data[idx] = &data[idx]
		}
		err := result.Scan(data...)
		if err != nil {
			return rows, errors.Wrap(err, "rows.Scan failed")
		}

		for idx, col := range columns {
			row[col] = data[idx]
		}

		rows = append(rows, row)
	}

	return rows, nil
}

func (db *MyDB) QueryColumn(table string, column string, options ...QueryOption) ([]Val, error) {
	var ret []Val

	query := NewQueryBuilder(table, options...)

	// 指定 column
	sqlStr, vals, err := query.BuildQuery(ColumnQb(column))
	if err != nil {
		return ret, errors.Wrap(err, "query.BuildQuery failed")
	}

	result, err := db.DB.Query(sqlStr, vals...)
	if err != nil {
		return ret, errors.Wrap(err, "db.Query failed")
	}

	for result.Next() {
		var col interface{}

		err := result.Scan(&col)
		if err != nil {
			return ret, errors.Wrap(err, "rows.Scan failed")
		}

		ret = append(ret, col)
	}

	return ret, nil
}

func (db *MyDB) QueryValue(table string, column string, options ...QueryOption) (Val, error) {
	var ret Val

	query := NewQueryBuilder(table, options...)

	// 指定 columns，强制 limit 1
	sqlStr, vals, err := query.BuildQuery(ColumnQb(column), LimitQb(0, 1))
	if err != nil {
		return ret, errors.Wrap(err, "query.BuildQuery failed")
	}

	err = db.DB.QueryRow(sqlStr, vals...).Scan(&ret)
	if err != nil {
		if err == sql.ErrNoRows && db.NoRowRtnZero {
			return ret, nil
		}
		return ret, errors.Wrap(err, "rows.Scan failed")
	}

	return ret, nil
}
