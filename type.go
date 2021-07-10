package sqlme

import (
	"database/sql"
)

type MyDB struct {
	DB *sql.DB

	NoRowRtnZero bool // 查询结果：没找到则返回错误
}

type Val interface{}

type Row map[string]interface{}

const (
	AndOr_And = "and"
	AndOr_OR  = "or"

	Type_Gen = "general_type"
	Type_Sub = "sub_types"

	Oper_Gt      = ">"
	Oper_Egt     = ">="
	Oper_Et      = "="
	Oper_NotEt   = "!="
	Oper_Lt      = "<"
	Oper_Elt     = "<="
	Oper_Like    = "like"
	Oper_NotLike = "not like"
	Oper_In      = "in"
	Oper_NotIn   = "not in"
	Oper_Between = "between"
)

type Where struct {
	Column   string
	Operator string
	Value    interface{}
	AndOr    string
	Type     string
}

type Data map[string]interface{}

type QueryBuilder struct {
	Distinct bool
	Table    string
	Columns  []string
	Wheres   []Where
	Limits   []int
	OrderBys []string
	Datas    Data
}
