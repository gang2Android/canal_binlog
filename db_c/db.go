package db_c

import (
	"canal_binlog/pub"
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"strconv"
	"strings"
	"time"
)

type Bean struct {
	Id string
}

type DbConfig struct {
	Addr  string
	User  string
	Pwd   string
	DB    string
	Table string
	Row   string
	// 命令：show binary logs;
	BinLog    string
	BinLogPos uint32
}

type MyEventHandler struct {
	canal.DummyEventHandler
}

var dbConfig DbConfig

func SetDbConfig(config DbConfig) {
	dbConfig = config
}

// OnRow 监听数据记录
func (h *MyEventHandler) OnRow(ev *canal.RowsEvent) error {
	switch ev.Table.Name {
	case "list":
		execGoods(ev)
	default:
		//fmt.Println("db_OnRow_" + ev.Table.Name)
	}
	return nil
}

func execGoods(ev *canal.RowsEvent) {
	id := ""
	for columnIndex, currColumn := range ev.Table.Columns {
		value := ev.Rows[len(ev.Rows)-1][columnIndex]
		switch currColumn.Name {
		case "id":
			id = strconv.Itoa(int(value.(uint32)))
		}
	}
	Pro := Bean{
		Id: id,
	}
	jsonStr, _ := json.Marshal(Pro)
	fmt.Println(time.Now(), jsonStr)
	pub.Sub.PublishPub(string(jsonStr))
}

// OnTableChanged 创建、更改、重命名或删除表时触发，通常会需要清除与表相关的数据，如缓存。It will be called before OnDDL.
func (h *MyEventHandler) OnTableChanged(schema string, table string) error {
	////库，表
	//record := fmt.Sprintf("%s %s \n", schema, table)
	//fmt.Println("OnTableChanged", record)
	return nil
}

// OnPosSynced 监听binlog日志的变化文件与记录的位置
func (h *MyEventHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	////源码：当force为true，立即同步位置
	//record := fmt.Sprintf("%v %v \n", pos.Name, pos.Pos)
	//fmt.Println("OnPosSynced", record)
	return nil
}

// OnRotate 当产生新的binlog日志后触发(在达到内存的使用限制后（默认为 1GB），会开启另一个文件，每个新文件的名称后都会有一个增量。)
func (h *MyEventHandler) OnRotate(r *replication.RotateEvent) error {
	////record := fmt.Sprintf("On Rotate: %v \n",&mysql.Position{Name: string(r.NextLogName), Pos: uint32(r.Position)})
	////binlog的记录位置，新binlog的文件名
	//record := fmt.Sprintf("On Rotate %v %v \n", r.Position, r.NextLogName)
	//fmt.Println("OnRotate", record)
	return nil

}

// OnDDL create alter drop truncate(删除当前表再新建一个一模一样的表结构)
func (h *MyEventHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	////binlog日志的变化文件与记录的位置
	//record := fmt.Sprintf("%v %v\n", nextPos.Name, nextPos.Pos)
	//query_event := fmt.Sprintf("%v\n %v\n %v\n %v\n %v\n",
	//	queryEvent.ExecutionTime,         //猜是执行时间，但测试显示0
	//	string(queryEvent.Schema),        //库名
	//	string(queryEvent.Query),         //变更的sql语句
	//	string(queryEvent.StatusVars[:]), //测试显示乱码
	//	queryEvent.SlaveProxyID) //从库代理ID？
	//fmt.Println("OnDDL:", record, query_event)
	return nil
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}

func Run() {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = dbConfig.Addr
	cfg.User = dbConfig.User
	cfg.Password = dbConfig.Pwd

	cfg.Dump.ExecutionPath = ""
	cfg.Dump.TableDB = dbConfig.DB
	cfg.Dump.Tables = strings.Split(dbConfig.Table, ",")

	c, err := canal.NewCanal(cfg)
	if err != nil {
		fmt.Println("error", err)
		return
	}

	c.SetEventHandler(&MyEventHandler{})

	fmt.Println("db Go run")
	startPos := mysql.Position{Name: dbConfig.BinLog, Pos: dbConfig.BinLogPos}
	c.RunFrom(startPos)

}
