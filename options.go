package sqlme

type MyDBOption func(*MyDB)

func NoRowRtnResult(rtnZero bool) MyDBOption {
	return func(mydb *MyDB) {
		mydb.NoRowRtnZero = rtnZero
	}
}

type QueryOption func(*QueryBuilder)

func WhereDistinct(distinct bool) QueryOption {
	return func(qb *QueryBuilder) {
		qb.Distinct = distinct
	}
}

func ColumnQb(columns ...string) QueryOption {
	return func(qb *QueryBuilder) {
		qb.Columns = columns
	}
}

func WhereQb(wheres ...Where) QueryOption {
	return func(qb *QueryBuilder) {
		qb.Wheres = wheres
	}
}

func LimitQb(limits ...int) QueryOption {
	return func(qb *QueryBuilder) {
		qb.Limits = limits
	}
}

func OrderByQb(orderBy ...string) QueryOption {
	return func(qb *QueryBuilder) {
		qb.OrderBys = orderBy
	}
}

func DataQb(data Data) QueryOption {
	return func(qb *QueryBuilder) {
		qb.Datas = data
	}
}
