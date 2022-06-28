package db

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/ini.v1"
)

type Db struct {
	driver *sql.DB
	err    error
	table  string
	sql    string
	where  []map[string]string
	method string
	limit  int64
	offset int64
	order  string
	fields string
	update map[string]string
	insert map[string]string
}

var (
	DbHost     string
	DbPort     string
	DbUser     string
	DbPassWord string
	DbName     string
	Prefix     string
)

func init() {
	file, err := ini.Load("config/config.ini")
	if err != nil {
		fmt.Println("配置文件读取错误，请检查文件路径:", err)
	}
	LoadData(file)
}
func LoadData(file *ini.File) {
	DbHost = file.Section("database").Key("DbHost").MustString("localhost")
	DbPort = file.Section("database").Key("DbPort").MustString("3306")
	DbUser = file.Section("database").Key("DbUser").MustString("ginblog")
	DbPassWord = file.Section("database").Key("DbPassWord").MustString("admin123")
	DbName = file.Section("database").Key("DbName").MustString("ginblog")
	Prefix = file.Section("database").Key("DbPrefix").MustString("")
}
func Table(table string) *Db {

	var db Db
	if len(Prefix) > 0 {
		table = Prefix + table
	}
	db.table = table
	db.driver, _ = GetDb()
	db.fields = "*"

	return &db
}
func (d *Db) Where(colume string, arg ...interface{}) *Db {
	var condition []map[string]string
	condition = make([]map[string]string, 1)
	if len(arg) == 1 {
		condition[0] = map[string]string{"colume": colume, "operate": "=", "value": strval(arg[0])}
	} else {
		condition[0] = map[string]string{"colume": colume, "operate": strval(arg[0]), "value": strval(arg[1])}
	}
	d.where = append(d.where, condition...)
	return d
}
func (d *Db) Fields(fields string) *Db {
	d.fields = fields
	return d
}
func (d *Db) Limit(arg ...int64) *Db {
	if len(arg) == 1 {
		d.limit = arg[0]
	} else {
		d.limit = arg[0]
		d.offset = arg[1]
	}
	return d
}
func (d *Db) Insert(data map[string]string) (bool, error) {
	d.insert = data
	d.formatSql("INSERT")
	if d.err != nil {
		return false, d.err
	}
	res := execute(d.driver, d.sql)
	if res == nil {
		return false, errors.New(d.sql + "：执行失败")
	}
	return true, nil
}
func (d *Db) Delete() (bool, error) {
	d.formatSql("DELETE")
	if d.err != nil {
		return false, d.err
	}
	res := execute(d.driver, d.sql)
	if res == nil {
		return false, errors.New(d.sql + "：执行失败")
	}
	return true, nil
}
func (d *Db) Update(data map[string]string) (bool, error) {
	d.update = data
	d.formatSql("UPDATE")
	if d.err != nil {
		return false, d.err
	}
	res := execute(d.driver, d.sql)
	if res == nil {
		return false, errors.New(d.sql + "：执行失败")
	}
	return true, nil
}
func (d *Db) FetchSql() string {
	return d.sql
}
func (d *Db) Find() (map[string]string, error) {
	d.formatSql("QUERY")
	if d.err != nil {
		err := d.err
		return nil, err
	}
	res := queryAndParse(d.driver, d.sql)
	return res, nil
}
func (d *Db) Select() ([]map[string]string, error) {
	d.formatSql("QUERY")
	if d.err != nil {
		err := d.err
		return nil, err
	}
	var res []map[string]string
	res = QueryAndParseRows(d.driver, d.sql)
	return res, nil
}
func (d *Db) formatSql(method string) {
	var where string
	var sql string
	for _, v := range d.where {
		if len(where) != 0 {
			where += " AND "
		}
		var condition = v
		switch condition["operate"] {
		case "=":
			where += condition["colume"] + "=" + condition["value"]
		case "like":
			where += condition["colume"] + " like " + condition["value"]
		case "<>":
			where += condition["colume"] + " != " + condition["value"]
		case ">":
			where += condition["colume"] + " > " + condition["value"]
		case "<":
			where += condition["colume"] + " < " + condition["value"]
		case "in":
			where += condition["colume"] + " in (" + condition["value"] + ")"
		}
	}
	switch method {
	case "QUERY":
		sql = fmt.Sprintf("select %s from %s", d.fields, d.table)
		if len(where) > 0 {
			sql += " where " + where
		}
		if d.limit > 0 && d.offset == 0 {
			sql += " limit " + strconv.FormatInt(int64(d.limit), 10)
		}
		if d.limit > 0 && d.offset > 0 {
			sql += " limit " + strconv.FormatInt(int64(d.limit), 10) + "," + strconv.FormatInt(int64(d.limit), 10)
		}
		if len(d.order) > 0 {
			sql += " order by " + d.order
		}
	case "UPDATE":
		if len(d.update) == 0 {
			d.err = errors.New("更新语句错误")
			return
		}
		if len(where) == 0 {
			d.err = errors.New("更新条件不能为空")
			return
		}
		var setValue string
		for key, value := range d.update {
			setValue += key + "='" + value + "',"
		}
		sql = fmt.Sprintf("update %s set %s where %s", d.table, strings.Trim(setValue, ","), where)
	case "DELETE":
		if len(where) == 0 {
			d.err = errors.New("删除条件不能为空")
			return
		}
		sql = fmt.Sprintf("delete from %s where %s", d.table, where)
	case "INSERT":
		if len(d.insert) == 0 {
			d.err = errors.New("新增内容不能为空")
			return
		}
		var insertKey string
		var insertValue string
		for key, value := range d.insert {
			insertKey += key + ","
			insertValue += "'" + value + "'" + ","
		}
		sql = fmt.Sprintf("insert into %s (%s) value (%s)", d.table, strings.Trim(insertKey, ","), strings.Trim(insertValue, ","))
	}
	d.sql = sql
}

//获取数据库控制器
func GetDb() (*sql.DB, error) {

	//DSN (Data Source Name)数据源连接格式:[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	//这里我们可以不选择数据库,或者增加可选参数,比如timeout(建立连接超时时间)
	//mysqlConnStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/mysql?&charset=utf8&parseTime=True&loc=Local&timeout=5s", username, password, host, port)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=5s", DbUser, DbPassWord, DbHost, DbPort, DbName)
	open, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("配置连接出错:%s\n", err.Error())
		return open, err
	}
	// 设置连接池中空闲连接的最大数量。
	open.SetMaxIdleConns(1)
	// 设置打开数据库连接的最大数量。
	open.SetMaxOpenConns(1)
	// 设置连接可复用的最大时间。
	open.SetConnMaxLifetime(time.Second * 30)
	//设置连接最大空闲时间
	open.SetConnMaxIdleTime(time.Second * 30)

	//检查连通性
	err = open.Ping()
	if err != nil {
		log.Printf("数据库连接出错:%s\n", err.Error())
		return open, err
	}

	return open, err
}

func execute(driver *sql.DB, queryStr string) interface{} {
	res, err := driver.Exec(queryStr)
	if err != nil {
		log.Printf("查询出错,SQL语句:%s\n错误详情:%s\n", queryStr, err.Error())
		return nil
	}
	defer driver.Close()
	return res
}

//单行数据解析 查询数据库，解析查询结果，支持动态行数解析
func queryAndParse(driver *sql.DB, queryStr string) map[string]string {
	rows, err := driver.Query(queryStr)
	if err != nil {
		log.Printf("查询出错,SQL语句:%s\n错误详情:%s\n", queryStr, err.Error())
		return nil
	}
	defer rows.Close()
	//获取列名cols
	cols, _ := rows.Columns()
	if len(cols) > 0 {
		buff := make([]interface{}, len(cols))       // 创建临时切片buff
		data := make([][]byte, len(cols))            // 创建存储数据的字节切片2维数组data
		dataKv := make(map[string]string, len(cols)) //创建dataKv, 键值对的map对象
		for i, _ := range buff {
			buff[i] = &data[i] //将字节切片地址赋值给临时切片,这样data才是真正存放数据
		}

		for rows.Next() {
			rows.Scan(buff...) // ...是必须的,表示切片
		}

		for k, col := range data {
			dataKv[cols[k]] = string(col)
			//fmt.Printf("%30s:\t%s\n", cols[k], col)
		}
		return dataKv
	} else {
		return nil
	}
}

//多行数据解析
func QueryAndParseRows(Db *sql.DB, queryStr string) []map[string]string {
	rows, err := Db.Query(queryStr)
	if err != nil {
		fmt.Printf("查询出错:\nSQL:\n%s, 错误详情:%s\n", queryStr, err.Error())
		return nil
	}
	defer rows.Close()
	//获取列名cols
	cols, _ := rows.Columns()
	if len(cols) > 0 {
		var ret []map[string]string
		for rows.Next() {
			buff := make([]interface{}, len(cols))
			data := make([][]byte, len(cols)) //数据库中的NULL值可以扫描到字节中
			for i, _ := range buff {
				buff[i] = &data[i]
			}
			rows.Scan(buff...) //扫描到buff接口中，实际是字符串类型data中
			//将每一行数据存放到数组中
			dataKv := make(map[string]string, len(cols))
			for k, col := range data { //k是index，col是对应的值
				//fmt.Printf("%30s:\t%s\n", cols[k], col)
				dataKv[cols[k]] = string(col)
			}
			ret = append(ret, dataKv)
		}
		return ret
	} else {
		return nil
	}
}

//任意可序列化数据转为Json,便于查看
func Data2Json(anyData interface{}) string {
	JsonByte, err := json.Marshal(anyData)
	if err != nil {
		log.Printf("数据序列化为json出错:\n%s\n", err.Error())
	}
	return string(JsonByte)
}
func strval(value interface{}) string {
	var key string
	if value == nil {
		return key
	}

	switch value.(type) {
	case float64:
		ft := value.(float64)
		key = strconv.FormatFloat(ft, 'f', -1, 64)
	case float32:
		ft := value.(float32)
		key = strconv.FormatFloat(float64(ft), 'f', -1, 64)
	case int:
		it := value.(int)
		key = strconv.Itoa(it)
	case uint:
		it := value.(uint)
		key = strconv.Itoa(int(it))
	case int8:
		it := value.(int8)
		key = strconv.Itoa(int(it))
	case uint8:
		it := value.(uint8)
		key = strconv.Itoa(int(it))
	case int16:
		it := value.(int16)
		key = strconv.Itoa(int(it))
	case uint16:
		it := value.(uint16)
		key = strconv.Itoa(int(it))
	case int32:
		it := value.(int32)
		key = strconv.Itoa(int(it))
	case uint32:
		it := value.(uint32)
		key = strconv.Itoa(int(it))
	case int64:
		it := value.(int64)
		key = strconv.FormatInt(it, 10)
	case uint64:
		it := value.(uint64)
		key = strconv.FormatUint(it, 10)
	case string:
		key = value.(string)
	case []byte:
		key = string(value.([]byte))
	default:
		newValue, _ := json.Marshal(value)
		key = string(newValue)
	}

	return key
}
