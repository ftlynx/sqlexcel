package main

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/ftlynx/tsx"
	"github.com/ftlynx/tsx/mysqlx"
	_ "github.com/go-sql-driver/mysql"
	"github.com/tealeg/xlsx"
	"gopkg.in/gomail.v2"
	"io/ioutil"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Config struct {
	Data  *DataConfig `toml:"data"`
	EMail *EmailConf  `toml:"email"`
}

type DataConfig struct {
	Datasource string `toml:"datasource"`
	Sql        string `toml:"sql"`
	Subject    string `toml:"subject"`
	Mailto     string `toml:"mailto"`
	Cc         string `toml:"cc"`
	PlainBody  string `toml:"plain_body"`
	AttachName string `toml:"attach_name"`
}

type EmailConf struct {
	Smtp   string `toml:"smtp"`
	Port   int    `toml:"port"`
	User   string `toml:"user"`
	Passwd string `toml:"passwd"`
}

func parseConfig(configpath string) (*Config, error) {
	cfg := new(Config)

	configPath, err := filepath.Abs(configpath)
	if err != nil {
		return cfg, fmt.Errorf("get config file absolute path failed, %s", err.Error())
	}

	file, err := os.Open(configPath)
	defer file.Close()
	if err != nil {
		return cfg, fmt.Errorf("open config file error, %s", err.Error())
	}

	fd, err := ioutil.ReadAll(file)
	if err != nil {
		return cfg, fmt.Errorf("read config file error, %s", err.Error())
	}

	cfg.Data = new(DataConfig)
	cfg.EMail = new(EmailConf)

	if err := toml.Unmarshal(fd, cfg); err != nil {
		return cfg, fmt.Errorf("load config file error, %s", err.Error())
	}

	return cfg, nil
}

type myDB struct {
	DB *sql.DB
}

type RowData []interface{}

func (d *myDB) QueryDataToMap(sql string) ([]string, []map[string]interface{}, error) {
	result := make([]map[string]interface{}, 0)
	rows, err := d.DB.Query(sql)
	if err != nil {
		return make([]string, 0), result, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return columns, result, err
	}

	r := make(RowData, len(columns))
	for index, _ := range r {
		var tmp interface{}
		r[index] = &tmp
	}

	for rows.Next() {
		if err := rows.Scan(r...); err != nil {
			return columns, result, err
		}
		item := make(map[string]interface{})
		for i, data := range r {
			item[columns[i]] = *data.(*interface{})
		}
		result = append(result, item)
	}
	return columns, result, err
}

func (d *myDB) QueryDataToSlice(sql string) ([]string, []RowData, error) {
	result := make([]RowData, 0)
	rows, err := d.DB.Query(sql)
	if err != nil {
		return make([]string, 0), result, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return columns, result, err
	}

	for rows.Next() {
		//初始化
		r := make(RowData, len(columns))
		for index, _ := range r {
			var tmp interface{}
			r[index] = &tmp
		}
		if err := rows.Scan(r...); err != nil {
			return columns, result, err
		}
		result = append(result, r)
	}
	return columns, result, err
}

func CreateExcelFromMap(columns []string, data []map[string]interface{}, filename string) error {
	file := xlsx.NewFile()
	sheet, err := file.AddSheet("sheet1")
	if err != nil {
		return err
	}
	row := sheet.AddRow()
	row.SetHeightCM(1)
	for _, v := range columns {
		cell := row.AddCell()
		cell.Value = v
	}
	for _, v1 := range data {
		row := sheet.AddRow()
		row.SetHeightCM(1)
		for _, column := range columns {
			cell := row.AddCell()
			cell.Value = fmt.Sprintf("%s", v1[column])
		}
	}
	return file.Save(filename)
}

func CreateExcelFromSlice(columns []string, data []RowData, filename string) error {
	file := xlsx.NewFile()
	sheet, err := file.AddSheet("sheet1")
	if err != nil {
		return err
	}
	row := sheet.AddRow()
	row.SetHeightCM(1)
	for _, v := range columns {
		cell := row.AddCell()
		cell.Value = v
	}
	for k1, _ := range data {
		row := sheet.AddRow()
		row.SetHeightCM(1)
		for k2, _ := range columns {
			cell := row.AddCell()
			cell.Value = fmt.Sprintf("%s", *data[k1][k2].(*interface{}))
		}
	}
	return file.Save(filename)
}

type connMail struct {
	User   string `json:"user"`
	Passwd string `json:"passwd"`
	Smtp   string `json:"smtp"`
	Port   int    `json:"port"`
}

func (m *connMail) Send(to string, cc string, subject string, plain_body string, attach_file string) error {
	d := gomail.NewDialer(m.Smtp, m.Port, m.User, m.Passwd)
	d.TLSConfig = &tls.Config{InsecureSkipVerify: true}

	//设置消息
	msg := gomail.NewMessage(gomail.SetCharset("UTF-8"), gomail.SetEncoding(gomail.Base64))
	msg.SetHeader("From", msg.FormatAddress(m.User, "系统邮件"))
	msg.SetHeader("To", strings.Split(to, ";")...)
	if cc != "" {
		msg.SetHeader("Cc", strings.Split(cc, ";")...)
	}
	msg.SetHeader("Subject", subject)
	msg.SetBody("text/plain", plain_body)

	if tsx.FileExists(attach_file) {
		names := strings.Split(attach_file, "/")
		name := names[len(names)-1]
		//处理附件文件名乱码
		fileHeader := map[string][]string{
			"Content-Type": {fmt.Sprintf(`text/plain; name="%s"`, mime.BEncoding.Encode("UTF-8", name))},
			//"Content-Disposition": {fmt.Sprintf(`attachment; filename="%s"`, mime.QEncoding.Encode("UTF-8", name))},
		}
		msg.Attach(attach_file, gomail.Rename(name), gomail.SetHeader(fileHeader))
	}

	return d.DialAndSend(msg)
}

func main() {
	if len(os.Args) < 2 {
		panic(fmt.Sprintf("usage %s config.ini", os.Args[0]))
	}
	cfg, err := parseConfig(os.Args[1])
	if err != nil {
		panic(err)
	}
	maxConnNum := 10
	maxIdleConn := 5
	maxLifeTime := time.Duration(int64(86400))
	db, err := mysqlx.SqlDB(cfg.Data.Datasource, maxConnNum, maxIdleConn, maxLifeTime)
	if err != nil {
		panic(err)
	}
	filename := fmt.Sprintf("/tmp/%s.xlsx", cfg.Data.AttachName)
	sqldata := myDB{DB: db}
	// 使用slice
	columns, result, err := sqldata.QueryDataToSlice(cfg.Data.Sql)
	if err != nil {
		panic(err)
	}
	if err := CreateExcelFromSlice(columns, result, filename); err != nil {
		panic(err)
	}
	/*
		// 使用map
		columns, result, err := sqldata.QueryDataToMap(cfg.Data.Sql)
		if err != nil {
			panic(err)
		}
		if err := CreateExcelFromMap(columns, result, filename); err != nil {
			panic(err)
		}
	*/
	email := connMail{
		User:   cfg.EMail.User,
		Passwd: cfg.EMail.Passwd,
		Smtp:   cfg.EMail.Smtp,
		Port:   cfg.EMail.Port,
	}
	if err := email.Send(cfg.Data.Mailto, cfg.Data.Cc, cfg.Data.Subject, cfg.Data.PlainBody, filename); err != nil {
		panic(err)
	}
	return
}
