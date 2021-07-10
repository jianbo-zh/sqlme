package sqlme_test

import (
	"testing"

	so "github.com/jianbo-zh/sqlme"
	_ "github.com/mattn/go-sqlite3"
)

func TestQueryBuilder(t *testing.T) {
	// query
	qb := so.NewQueryBuilder("tabs",
		so.WhereQb(
			so.Where{Column: "name", Operator: "like", Value: "you"},
			so.Where{Column: "age", Operator: ">", Value: 19, AndOr: so.AndOr_OR},
		),
		so.OrderByQb("id asc"),
		so.LimitQb(0, 10),
	)

	sql, vals, err := qb.BuildQuery(so.ColumnQb("id", "name", "age"))
	if err != nil {
		t.Error(err)
	}

	t.Log(sql, vals)

	qb = so.NewQueryBuilder("tabs",
		so.WhereQb(
			so.Where{Column: "name", Operator: "like", Value: "you"},
			so.Where{Column: "age", Operator: ">", Value: 19, AndOr: so.AndOr_OR},
		),
		so.DataQb(so.Data{
			"name":   "nick",
			"gender": 1,
		}),
		so.LimitQb(1),
	)

	sql, vals, err = qb.BuildUpdate()
	if err != nil {
		t.Error(err)
	}
	t.Log(sql, vals)

	qb = so.NewQueryBuilder("tabs",
		so.DataQb(so.Data{
			"name":   "nick",
			"gender": 1,
		}),
	)

	sql, vals, err = qb.BuildCreate()
	if err != nil {
		t.Error(err)
	}
	t.Log(sql, vals)

	qb = so.NewQueryBuilder("tabs",
		so.WhereQb(
			so.Where{Column: "name", Operator: "like", Value: "you"},
			so.Where{AndOr: so.AndOr_And, Type: so.Type_Sub, Value: []so.Where{
				{Column: "age", Operator: "<", Value: "14"},
				{Column: "age", Operator: ">", Value: "76", AndOr: so.AndOr_OR},
			}},
		),
		so.LimitQb(1),
	)

	sql, vals, err = qb.BuildDelete()
	if err != nil {
		t.Error(err)
	}
	t.Log(sql, vals)
}
