package main

import (
	"path/filepath"
	"time"
	"fmt"
	"strconv"
	"reflect"
	"github.com/hpcloud/tail"
	"github.com/zngw/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
)

var db *leveldb.DB

func getleveldb(path string) int64 {
    var err error
	db, err = leveldb.OpenFile("leveldb", nil)
// 	defer db.Close()
	if err != nil {
	    log.Error("sys","leveldb.OpenFile failed, err:%v", err)
	    return 0
	}
	val, err := db.Get([]byte(path), nil)
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

//整个for循环，推送数据到channel，消费
func main() {
    
    
    
	// 启动用tail监听
	frpLog, _ := filepath.Abs("request.log")
	
    t := getleveldb(frpLog)
	
	//开始
	tails, err := tail.TailFile(frpLog, tail.Config{
		ReOpen:    true,                                 // 重新打开
		Follow:    true,                                 // 是否跟随
		Location:  &tail.SeekInfo{Offset: t, Whence: 0}, // 从文件的哪个地方开始读
		MustExist: true,                                // 文件不存在不报错
		Poll:      true,
	})
	if err != nil {
		log.Error("sys","tail file failed, err:%v", err)
		return
	}
	
	log.Trace("sys", "frptables 已启动，正在监听日志文件：%s", frpLog)
	var line *tail.Line
	var ok bool

	for {
		line, ok = <-tails.Lines
		fmt.Println(line)
		
		if !ok {
			log.Error("sys","tail file close reopen, filename:%s\n", tails.Filename)
			time.Sleep(time.Second)
			continue
		}
		//保存点位
		temp,_ := tails.Tell()
		db.Put([]byte(frpLog), []byte(strconv.Itoa(int(temp))), nil)
	}
	
	
	
	
	
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
