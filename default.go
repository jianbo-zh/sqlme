package sqlme

import (
	"github.com/pkg/errors"
)

var dftDB *MyDB

func InitDbConn(drv, dsn string, options ...MyDBOption) error {
	mydb, err := NewDB(drv, dsn, options...)
	if err != nil {
		return errors.Wrap(err, "NewDB failed")
	}
	dftDB = mydb

	return nil
}

// Insert
func Insert(table string, datas Data) (int64, error) {
	return dftDB.Insert(table, datas)
}

// InsertBatch 批量插入
func InsertBatch(table string, datas []Data) (int64, error) {
	return dftDB.InsertBatch(table, datas)
}

// InsertRaw sql 语句插入
func InsertRaw(sql string, vals ...interface{}) (int64, error) {
	return dftDB.InsertRaw(sql, vals)
}

// Delete 删除满足条件所有行
func Delete(table string, wheres ...Where) (int64, error) {
	return dftDB.Delete(table, wheres...)
}

// DeleteLimit 删除指定行数
func DeleteLimit(table string, limit []int, wheres ...Where) (int64, error) {
	return dftDB.DeleteLimit(table, limit, wheres...)
}

// DeleteRaw sql 语句删除
func DeleteRaw(sql string, vals ...interface{}) (int64, error) {
	return dftDB.DeleteRaw(sql, vals...)
}

// Update 全部更新
func Update(table string, data Data, wheres ...Where) (int64, error) {

	return dftDB.Update(table, data, wheres...)
}

// UpdateLimit 指定更新行数
func UpdateLimit(table string, data Data, limit []int, wheres ...Where) (int64, error) {
	return dftDB.UpdateLimit(table, data, limit, wheres...)
}

// UpdateRaw sql 语句更新
func UpdateRaw(sql string, vals ...interface{}) (int64, error) {
	return dftDB.UpdateRaw(sql, vals...)
}

// Query 查询多行
func Query(table string, columns []string, options ...QueryOption) ([]Row, error) {
	return dftDB.Query(table, columns, options...)
}

// QueryRow 查询一行
func QueryRow(table string, columns []string, options ...QueryOption) (Row, error) {
	return dftDB.QueryRow(table, columns, options...)
}

// QueryRaw sql 语句查询
func QueryRaw(sql string, vals ...interface{}) ([]Row, error) {
	return dftDB.QueryRaw(sql, vals...)
}
