package sqlme_test

import (
	"testing"

	so "github.com/jianbo-zh/sqlme"
	_ "github.com/mattn/go-sqlite3"
)

type User struct {
	ID    string `db:"id"`
	Name  string `db:"name"`
	CTime int64  `db:"ptime"`
}

func TestDB(t *testing.T) {
	db, err := so.NewDB("sqlite3", "D:\\Projects\\sqlme\\foo.db")
	if err != nil {
		t.Error(err)
	}

	vals, err := db.QueryColumn("sys_user", "id", so.LimitQb(5), so.OrderByQb("id desc"))
	if err != nil {
		t.Error(err)
	}

	t.Log("db.QueryColumn:", vals)

	val, err := db.QueryValue("sys_user", "id", so.OrderByQb("id desc"))
	if err != nil {
		t.Error(err)
	}

	t.Log("db.QueryValue1:", val)

	val, err = db.QueryValue("sys_user", "id", so.OrderByQb("id desc"), so.WhereQb(so.Where{
		Column:   "id",
		Operator: so.Oper_Et,
		Value:    0,
	}))
	if err != nil {
		t.Error(err)
	}

	t.Log("db.QueryValue2:", val)

	rows, err := db.Query("sys_user", []string{"id", "nickname"}, so.LimitQb(2), so.OrderByQb("id desc"))
	if err != nil {
		t.Error(err)
	}

	t.Log(rows)

	var users []User
	rows, err = db.Query("sys_user", []string{"id", "name", "ptime"}, so.LimitQb(3))
	if err != nil {
		panic(err)
	}

	err = so.ParseRows(rows, &users)
	if err != nil {
		panic(err)
	}

	t.Log("Users:", users)

	row, err := db.QueryRow("sys_user", []string{"id", "nickname"}, so.OrderByQb("id desc"))
	if err != nil {
		t.Error(err)
	}

	t.Log(row)

	num, err := db.Delete("sys_user", so.Where{
		Column: "id", Operator: "=", Value: "123",
	})
	if err != nil {
		t.Error(err)
	}

	t.Log(num)

	iid, err := db.Insert("sys_user", so.Data{
		"id":       "123",
		"peer_id":  "peer_id",
		"name":     "name",
		"phone":    13712342345,
		"nickname": "nickname",
		"img":      "img",
	})
	if err != nil {
		t.Error(err)
	}

	t.Log(iid)

	num, err = db.Update("sys_user",
		so.Data{
			"peer_id": "peer_id111",
			"img":     "img123213",
		},
		so.Where{Column: "id", Operator: "=", Value: "123"},
		so.Where{AndOr: so.AndOr_OR, Column: "name", Operator: "like", Value: "name"},
	)
	if err != nil {
		t.Error(err)
	}

	t.Log(num)
}
