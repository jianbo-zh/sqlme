package sqlme

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

func parseSqlColumns(sql string) ([]string, error) {
	cols := []string{}

	strArr := strings.Split(strings.TrimPrefix(strings.ToLower(sql), "select"), "from")
	if len(strArr) == 0 {
		return cols, errors.New("sql failed")
	}

	lFields := strings.Split(strArr[0], ",")

	for _, val := range lFields {
		fArr := strings.Split(val, "as")
		var col string
		if len(fArr) == 1 {
			col = strings.TrimSpace(fArr[0])
		} else if len(fArr) == 2 {
			col = strings.TrimSpace(fArr[1])
		} else {
			return cols, errors.New("sql failed")
		}
		if col == "*" {
			return cols, errors.New("can not support [select * from]")
		}
		cols = append(cols, col)
	}

	return cols, nil
}

func ParseRows(rs []Row, ts interface{}) error {
	refTV := reflect.ValueOf(ts)

	if refTV.Kind() != reflect.Ptr {
		return errors.New("interface{} must be pointer to slice")

	} else if refTV.Elem().Kind() != reflect.Slice {
		return errors.New("interface{} must be pointer to slice")
	}

	tss := reflect.MakeSlice(refTV.Elem().Type(), len(rs), len(rs))

	for idx, val := range rs {
		x := tss.Index(idx).Addr().Interface()
		err := ParseRow(val, x)
		if err != nil {
			return err
		}
	}

	refTV.Elem().Set(tss)

	return nil
}

func ParseRow(r Row, t interface{}) error {
	refTV := reflect.ValueOf(t)

	if refTV.Kind() != reflect.Ptr {
		return errors.New("interface{} must be pointer")
	}

	fieldNum := refTV.Elem().NumField()
	for i := 0; i < fieldNum; i++ {
		if refTV.Elem().Field(i).CanSet() {
			tn := refTV.Elem().Type().Field(i).Tag.Get("db")
			if tn == "" {
				tn = strings.ToLower(refTV.Elem().Type().Field(i).Name)
			}

			if _, ok := r[tn]; !ok {
				continue
			}

			err := DecodeField(r[tn], refTV.Elem().Field(i).Addr())
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("decodeField failed:%s", tn))
			}

		}
	}

	return nil
}

func DecodeField(src interface{}, fieldV reflect.Value) error {

	if src == nil {
		return nil
	}

	if fieldV.Kind() != reflect.Ptr {
		return errors.New("fieldV must pointer")
	}

	if !fieldV.Elem().CanSet() {
		return errors.New("fieldV Elem must CanSet")
	}

	srcV := reflect.ValueOf(src)

	switch fieldV.Elem().Kind() {
	case reflect.Bool:
		switch srcV.Kind() {
		case reflect.Bool:
			fieldV.Elem().SetBool(srcV.Bool())
		case reflect.String:
			bl, err := strconv.ParseBool(srcV.String())
			if err != nil {
				return errors.Wrap(err, "strconv.ParseBool failed")
			}
			fieldV.Elem().SetBool(bl)
		case reflect.Float64, reflect.Float32:
			fieldV.Elem().SetBool(srcV.Float() != 0)
		case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
			fieldV.Elem().SetBool(srcV.Int() != 0)
		case reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Uint:
			fieldV.Elem().SetBool(srcV.Uint() != 0)
		default:
			return errors.New(fmt.Sprintf("unsupport convert %s to %s", srcV.Kind(), reflect.Bool))
		}
	case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
		switch srcV.Kind() {
		case reflect.Bool:
			if srcV.Bool() {
				fieldV.Elem().SetInt(1)
			} else {
				fieldV.Elem().SetInt(0)
			}
		case reflect.String:
			ii, err := strconv.ParseInt(srcV.String(), 10, 64)
			if err != nil {
				return errors.Wrap(err, "strconv.ParseInt failed")
			}
			fieldV.Elem().SetInt(ii)
		case reflect.Float64, reflect.Float32:
			fieldV.Elem().SetInt(int64(srcV.Float()))
		case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
			fieldV.Elem().SetInt(srcV.Int())
		case reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Uint:
			fieldV.Elem().SetInt(int64(srcV.Uint()))
		default:
			return errors.New(fmt.Sprintf("unsupport convert %s to %s", srcV.Kind(), reflect.Int))
		}
	case reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Uint:
		switch srcV.Kind() {
		case reflect.Bool:
			if srcV.Bool() {
				fieldV.Elem().SetUint(1)
			} else {
				fieldV.Elem().SetUint(0)
			}
		case reflect.String:
			ii, err := strconv.ParseUint(srcV.String(), 10, 64)
			if err != nil {
				return errors.Wrap(err, "strconv.ParseInt failed")
			}
			fieldV.Elem().SetUint(ii)
		case reflect.Float64, reflect.Float32:
			fieldV.Elem().SetUint(uint64(srcV.Float()))
		case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
			fieldV.Elem().SetUint(uint64(srcV.Int()))
		case reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Uint:
			fieldV.Elem().SetUint(srcV.Uint())
		default:
			return errors.New(fmt.Sprintf("unsupport convert %s to %s", srcV.Kind(), reflect.Uint))
		}
	case reflect.Float32, reflect.Float64:
		switch srcV.Kind() {
		case reflect.Bool:
			if srcV.Bool() {
				fieldV.Elem().SetFloat(1)
			} else {
				fieldV.Elem().SetFloat(0)
			}
		case reflect.String:
			ff, err := strconv.ParseFloat(srcV.String(), 64)
			if err != nil {
				return errors.Wrap(err, "strconv.ParseInt failed")
			}
			fieldV.Elem().SetFloat(ff)
		case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
			fieldV.Elem().SetFloat(float64(srcV.Int()))
		case reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Uint:
			fieldV.Elem().SetFloat(float64(srcV.Uint()))
		case reflect.Float32, reflect.Float64:
			fieldV.Elem().SetFloat(srcV.Float())
		default:
			return errors.New(fmt.Sprintf("unsupport convert %s to %s or %s", srcV.Kind(), reflect.Float32, reflect.Float64))
		}
	case reflect.String:
		switch srcV.Kind() {
		case reflect.Bool:
			if srcV.Bool() {
				fieldV.Elem().SetString("true")
			} else {
				fieldV.Elem().SetString("false")
			}
		case reflect.String:
			fieldV.Elem().SetString(srcV.String())
		case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
			fieldV.Elem().SetString(strconv.FormatInt(srcV.Int(), 10))
		case reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Uint:
			fieldV.Elem().SetString(strconv.FormatUint(srcV.Uint(), 10))
		case reflect.Float32, reflect.Float64:
			fieldV.Elem().SetString(strconv.FormatFloat(srcV.Float(), 'f', -1, 64))
		default:
			return errors.New(fmt.Sprintf("unsupport convert %s to %s", srcV.Kind(), reflect.String))
		}
	default:
		return errors.New(fmt.Sprintf("unsupport type kind: %s", fieldV.Kind()))
	}

	return nil
}
