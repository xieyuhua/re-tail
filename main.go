package main

import (
    "database/sql"
    "encoding/json"
    "path/filepath"
    "bufio"
    "os"
    "time"
    "io/ioutil"
    "strings"
    "flag"
    "fmt"
    "strconv"
    "reflect"
    "github.com/hpcloud/tail"
    "github.com/zngw/log"
    "github.com/syndtr/goleveldb/leveldb"
    "github.com/syndtr/goleveldb/leveldb/errors"
    _ "github.com/go-sql-driver/mysql"
)


type configuration struct {
	ServerPort int    `json:"server_port"`
	Username   string `json:"mysql_username"`
	Password   string `json:"mysql_password"`
	Server     string `json:"mysql_server"`
	Database   string `json:"mysql_database"`
	Port       int    `json:"mysql_port"`
}

type tableinfo struct {
	Table  string   `json:"Table"`
	Fields []string `json:"Fields"`
	Alias  []string `json:"Alias"`
}

var tables map[string]json.RawMessage
var serverstring string
var port string
var verbose bool
var db *sql.DB


const version = "1.0.0"
const author = "seaslog xie"
var filedb *leveldb.DB


type Args struct {
	File  *string
	Table *string
	Verbose *bool
}

func getArgs() (Args, bool) {
	args := Args{}
	args.File    = flag.String("f", "request.log", " file path, content json format (require)")
	args.Table   = flag.String("t", "", "Table name (require)")
	args.Verbose = flag.Bool("d", false, "Verbose debug")
	flag.Parse()
	isFlagPassed := func(name string) bool {
		found := false
		flag.Visit(func(f *flag.Flag) {
			if f.Name == name {
				found = true
			}
		})
		return found
	}
	verbose = *args.Verbose
	found :=  isFlagPassed("f") && isFlagPassed("t")
	if !found {
		flag.Usage()
	}
	return args, found
}


//根据seek 划分文件
func newLog(path string, frpLog string, tails *tail.Tail) {
    //重新命名一个文件
    t := time.Now()
    t.AddDate(0,0,-1)
    str := fmt.Sprintf("%s%s", path, t.AddDate(0,0,-1).Format("2006-01-02"))
    _, err := os.Stat(str)
    if err != nil {
        if os.IsNotExist(err) {
            
        } else {
            return
        }
    } else {
       return 
    }
    seek := getleveldb(frpLog)
    if seek==0{
         return
    }
    filedb.Put([]byte(frpLog), []byte(strconv.Itoa(0)), nil)
    err = os.Rename(path, str)
    if err != nil {
        log.Error("err",  err.Error())
        return
    }
    //重新归零
    tails.SetSeekTo(0)
    // 打开文件
    file, err :=  os.OpenFile(str, os.O_RDWR|os.O_CREATE, os.ModePerm)
    if err != nil {
        log.Error("err",  err.Error())
        return
    }
    defer file.Close()
    // 创建Scanner对象
    scanner := bufio.NewScanner(file)
    // 定位到指定偏移量
    file.Seek(seek, 0)
    
    //创建文件
    newfile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
    if err != nil {
        log.Error("err",  "rror opening file:", err)
        return
    }
    defer newfile.Close()
    // 遍历文件，读取数据并输出
    write := bufio.NewWriter(newfile)
    for scanner.Scan() {
        line := scanner.Text()
         _, err = write.WriteString((line)+"\n")
        if err != nil {
            log.Error("err",  "Error writing to output file 1:", err)
            return
        }
        write.Flush()
    }
    
    if err := scanner.Err(); err != nil {
        log.Error("err",  err.Error())
        return
    }
    
    //截取文件
    err = file.Truncate(seek)
    if err != nil {
        log.Error("err",  err.Error())
        return
    }
    return
}
//获取点位
func getleveldb(path string) int64 {
	val, err := filedb.Get([]byte(path), nil)
	var t int64
	//重新开始
	if err == errors.ErrNotFound {
    	t = 0
	} else {
	    fmt.Println(string(val))
    	t ,_ = strconv.ParseInt(string(val), 10, 64)
	}
	return t
}

func init() {
    var err error
    
	filedb, err = leveldb.OpenFile("leveldb", nil)
	if err != nil {
	    log.Error("err","leveldb.OpenFile failed, err:%v", err)
	    return 
	}
    
	// load configruation file
	mysqlfullpath := "conf/config.json"
	mysqlfile, err := ioutil.ReadFile(mysqlfullpath)
	errorcheck(err)

	data := configuration{}
	err = json.Unmarshal([]byte(mysqlfile), &data)
	errorcheck(err)

	serverstring = data.Username + ":" + data.Password + "@tcp(" + data.Server + ":" + strconv.Itoa(data.Port) + ")/" + data.Database
	port = strconv.Itoa(data.ServerPort)

	// load tables.json file
	tablesfullpath := "conf/tables.json"
	tablesdata, err := ioutil.ReadFile(tablesfullpath)
	errorcheck(err)
	
    db, err = sql.Open("mysql", serverstring)
    errorcheck(err)
    db.SetMaxOpenConns(20)
    db.SetMaxIdleConns(20)
    pingDB(db)
	err = json.Unmarshal([]byte(tablesdata), &tables)
	errorcheck(err)
}


//整个for循环，推送数据到channel，消费
func main() {
    
	args, isok := getArgs()
	if !isok {
		return
	}
    defer filedb.Close()
	// 启动用tail监听
	frpLog, _ := filepath.Abs(*args.File)
	
	//分割日志，未读取完的保留数据
	//重启日志分割
	//因为TailFile  读取中文件，其他也在输入，需要修改seek位置置零
// 	newLog(*args.File, frpLog)
	
	t := getleveldb(frpLog)
	
	//开始
	tails, err := tail.TailFile(frpLog, tail.Config{
		ReOpen:    true,                                 // 重新打开
		Follow:    true,                                 // 是否跟随
		Location:  &tail.SeekInfo{Offset: t, Whence: 0}, // 从文件的哪个地方开始读
		MustExist: false,                                // 文件不存在不报错
		Poll:      true,
	})
	if err != nil {
		log.Error("sys","tail file failed, err:%v", err)
		return
	}
	
	log.Trace("sys", "re-tail 已启动，正在监听日志文件：%s", frpLog)
	var line *tail.Line
	var ok bool
	
    //读取数据
	for {
		line, ok = <-tails.Lines
    	if verbose {
    	    fmt.Println(line.Text)
    		fmt.Printf("bulk insert done -> %s \n", *args.Table)
    	}
		if !ok {
		   //重新归零
            // tails.SetSeekTo(0)
			log.Error("sys","tail file close reopen, filename:%s\n", tails.Filename)
			time.Sleep(time.Second)
			continue
		}
		
		//新增批量
    	mysqlInvokeBulk(*args.Table, []byte(line.Text))
    	
    	
    	//保存点位
    	temp,_ := tails.Tell()
		filedb.Put([]byte(frpLog), []byte(strconv.Itoa(int(temp))), nil)
		fmt.Println(strconv.Itoa(int(temp)))
		
		//b备份文件大小
		if int(temp) > 10*1204*1024 {
		    newLog(*args.File, frpLog ,tails)
		}
        // DoMethod(tails)
	}
}


func mysqlInvokeBulk(auth string, body []byte) {
	
	if verbose {
	    fmt.Printf("bulk -> %s : %s \n", auth, body)
	}
	result := "Failed"
	
	defer func() {
		if err := recover(); err != nil && verbose {
		    // 强结构形式
		    tempval := fmt.Sprintf("%s : %s", body, err)
		    log.Error("err",  string(body), tempval)
			fmt.Println(err)
		}
    	if verbose {
    		fmt.Println(result)
    	}
	}()
	
	if auth != "" && len(body) > 0 {
	    //数据
		if table,  values, err := passBody(auth, body); err != nil {
			errorcheck(err)
		} else {
		    //新增
			if err := passToMySQL(table,  values); err != nil {
				errorcheck(err)
			} else {
				result = "Ok"
			}
		}
	}
}

func passBody(auth string, body []byte) (table string, values map[string]interface{}, _ error) {
	// Get relevant table
	tableGUID := tables[auth]
	if len(tableGUID) == 0 {
	    log.Error("err",  string(body), errors.New("no such table"))
		return "", nil, errors.New("no such table")
	}

	tableinfo := tableinfo{}
	if err := json.Unmarshal(tableGUID, &tableinfo); err != nil {
	    log.Error("err",  string(body), errors.New("table.json issue"))
		return "",  nil, errors.New("table.json issue")
	}

	// Get table name / alias / fields
	table   = tableinfo.Table
	alias  := tableinfo.Alias
	fields := tableinfo.Fields

	if len(table) == 0 || len(alias) == 0 || len(fields) == 0 {
	    log.Error("err",  string(body), errors.New("issue with table values"))
		return "",  nil, errors.New("issue with table values")
	}
	
	var objmap map[string]any
	d := json.NewDecoder(strings.NewReader(string(body)))
	d.UseNumber()
	err := d.Decode(&objmap)
	if err != nil {
	    log.Error("err",  string(body), errors.New("load body issue"))
		return "",  nil, errors.New("load body issue")
	}
	
    values = make(map[string]any)
	// Pull data from body and put into comma delimited values
	for i := 0; i < len(alias); i++ {
		tempval := objmap[string(alias[i])]
		var str string
		switch tempval.(type) {
    		case int,uint,int8,uint8,int16,uint16,int32,uint32,int64,uint64:
    			str = strconv.Itoa(tempval.(int))
    			break
    		case float64,float32:
    			str = fmt.Sprintf("%f", tempval)
    			break
    		case string:
    			str = tempval.(string)
    			break
    		default:
    			str = fmt.Sprintf("%v", tempval)
		}
		values[fields[i]] = str
	}
	return table, values, nil
}

func passToMySQL(table string,  data map[string]interface{}) (err error) {

	value := make([]interface{}, 0, len(data)*2)
	fields  := ""
	values  := ""
	fields2 := ""

	for k, v := range data {
	    value = append(value, v) 
		if fields == ""{
			fields = "`"+k+"`"
			values = "?"
			fields2 = k+"= ?"
		}else{
			fields += ",`"+k+"`"
			values += ",?"
			fields2 += ","+k+"= ?"
		}
	}
    
	if len(value) == 0 {
	    log.Error("err", "no such len")
	    return errors.New("no such len")
	}
	//重复事情
	for _, v := range value {
	    value = append(value, v) 
	}
	
	sql := "INSERT INTO "+table+" ("+ fields +") VALUES ("+values+") ON DUPLICATE KEY UPDATE " + fields2
	stmt, err := db.Prepare(sql)
	defer stmt.Close() //关闭之
	if err != nil {
	    log.Error("err",  err.Error())
	    return errors.New(err.Error())
	}
	res, err := stmt.Exec(value...)
	if err != nil {
	    log.Error("err",  err.Error())
	    return errors.New(err.Error())
	}
	
	id, err := res.LastInsertId()
	if err != nil {
	    log.Error("err",  err.Error())
	    return errors.New(err.Error())
	}
    
	fmt.Println("Insert id", id)

	return err
}

func errorcheck(err error) {
	if err != nil {
		panic(err.Error())
	}
}

func pingDB(db *sql.DB) {
	err := db.Ping()
	errorcheck(err)
	return
}

// 通过接口来获取任意参数
func DoFiled(input interface{}) {
    getType := reflect.TypeOf(input) //先获取input的类型
    fmt.Println("Type is :", getType.Name()) // Person
    fmt.Println("Kind is : ", getType.Kind()) // struct
    getValue := reflect.ValueOf(input)
    fmt.Println("Fields is:", getValue) //{王富贵 20 男}
    // 获取方法字段
    // 1. 先获取interface的reflect.Type，然后通过NumField进行遍历
    // 2. 再通过reflect.Type的Field获取其Field
    // 3. 最后通过Field的Interface()得到对应的value
        for i := 0; i < getType.NumField(); i++ {
            field := getType.Field(i)
            value := getValue.Field(i).Interface() //获取第i个值
            fmt.Printf("字段名称:%-20s, 字段类型:%-20s, 字段数值:%-20s \n", field.Name, field.Type, value)
        }
}

// 通过接口来获取任意参数
func DoMethod(input interface{}) {
    getType := reflect.TypeOf(input) //先获取input的类型
    fmt.Println("Type is :", getType.Name()) // Person
    fmt.Println("Kind is : ", getType.Kind()) // struct
    // 通过反射，操作方法
    // 1. 先获取interface的reflect.Type，然后通过.NumMethod进行遍历
    // 2. 再公国reflect.Type的Method获取其Method
        for i := 0; i < getType.NumMethod(); i++ {
            method := getType.Method(i)
            fmt.Printf("方法名称:%-20s, 方法类型:%-20s \n", method.Name, method.Type)
        }
}
