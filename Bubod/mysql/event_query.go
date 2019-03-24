// documentation:
// https://dev.mysql.com/doc/internals/en/query-event.html
// The query event is used to send text querys right the binlog.
// 其余查询时间，向binlog发送文本查询
package mysql

import (
	"bytes"
	"encoding/binary"
)

// 执行更新语句时会生成 QUERY_EVENT，包括 create, insert, update, delete. 



type QueryEvent struct {
	header        EventHeader
	slaveProxyId  uint32    // 4字节。发起这个语句的线程id，对于临时表来说是必须的。这也有助于DBA知道谁在master上干了啥。
	executionTime uint32	// 4字节。语句执行的时长，单位为秒。只对于DBA的监控有用。
	errorCode     uint16	// 2字节。在master执行语句的错误码。错误码定义在include/mysqld_error.h文件中。0表示没有错误。
	schema        string 	// 默认数据库名
	statusVars    string    // 大于等于0的状态变量（v1、v3中不存在）。每个状态变量包含一个字节码，标识存储变量，后面跟着变量的值。
	query         string    // sql语句
}

func (parser *eventParser) parseQueryEvent(buf *bytes.Buffer) (event *QueryEvent, err error) {
	var schemaLength byte
	var statusVarsLength uint16

	event = new(QueryEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	err = binary.Read(buf, binary.LittleEndian, &event.slaveProxyId)
	err = binary.Read(buf, binary.LittleEndian, &event.executionTime)
	err = binary.Read(buf, binary.LittleEndian, &schemaLength)     		//1B
	err = binary.Read(buf, binary.LittleEndian, &event.errorCode)
	err = binary.Read(buf, binary.LittleEndian, &statusVarsLength)      //2B
	event.statusVars = string(buf.Next(int(statusVarsLength)))
	event.schema = string(buf.Next(int(schemaLength)))
	_, err = buf.ReadByte()
	event.query = buf.String()
	return
}