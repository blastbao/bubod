// https://dev.mysql.com/doc/internals/en/binlog-event-header.html
// 事件 header头信息
package mysql

import (
	"bytes"
	"encoding/binary"
	"fmt"
)




// binlog 实际上由一个个不同类型的 binlog event 组成，每个 binlog event 还包含了 EventHeader 和 EventData 部分(可选)。
//【注意：每个event最后还有4bytes的校验位，官方文档并没有提到这个地方，不然分析 event 物理格式时候会发现 event 长度对不上号】

// 常见的一个binlog物理文件有如下组成部分：
// 1、4字节的magic number作为binlog文件的开头
// 2、N个不同类型的binlog event
// 3、rotate event 作为binlog文件的结尾（正在使用的binlog里面是没有rotate event的）

// 下表就是的binlog event的一般格式：

// +=====================================+
// | event  | timestamp         0 : 4    |
// | header +----------------------------+
// |        | type_code         4 : 1    | = FORMAT_DESCRIPTION_EVENT = 15（binlog v4）
// |        +----------------------------+
// |        | server_id         5 : 4    |
// |        +----------------------------+
// |        | event_length      9 : 4    | >= 91
// |        +----------------------------+
// |        | next_position    13 : 4    |
// |        +----------------------------+
// |        | flags            17 : 2    |
// +=====================================+
// | event  | binlog_version   19 : 2    | = 4
// | data   +----------------------------+
// |        | server_version   21 : 50   |
// |        +----------------------------+
// |        | create_timestamp 71 : 4    |
// |        +----------------------------+
// |        | header_length    75 : 1    |
// |        +----------------------------+
// |        | post-header      76 : n    | = array of n bytes, one byte per event type that the server knows about
// |        | lengths for all            |   
// |        | event types                |
// +=====================================+

// 此外，还有一个索引文件记录当前有哪些binlog文件，及当前正在使用的binlog文件。（文件名类似：mysql-bin.index）

// 通用事件头(common-header)结构体 EventHeader ，固定为19个字节。
//
// 属性			字节数	含义
// timestamp	4		包含了该事件的开始执行时间
// eventType	1		事件类型: 是最重要的一个参数，不同的事件类型对应不同的 EventData 数据结构布局
// serverId		4		服务器标识，标识产生该事件的 MySQL 服务器的
// eventLength	4		该事件的长度: EventHeader + EventData + CheckSum
// nextPosition	4		下一个事件在binlog文件中的位置
// flags		2		标识产生该事件的 MySQL 服务器的 server-id

type EventHeader struct {
	Timestamp uint32	   
	EventType EventType	
	ServerId  uint32	
	EventSize uint32	
	LogPos    uint32	
	Flags     eventFlag 
}

func (header *EventHeader) Read(data []byte) error {
	buf := bytes.NewBuffer(data)
	return binary.Read(buf, binary.LittleEndian, header)
}

func (header *EventHeader) EventName() string {
	switch header.EventType {
	case UNKNOWN_EVENT:
		return "UNKNOWN_EVENT"
	case START_EVENT_V3:
		return "START_EVENT_V3"
	case QUERY_EVENT:
		return "QUERY_EVENT"
	case STOP_EVENT:
		return "STOP_EVENT"
	case ROTATE_EVENT:
		return "ROTATE_EVENT"
	case INTVAR_EVENT:
		return "INTVAR_EVENT"
	case LOAD_EVENT:
		return "LOAD_EVENT"
	case SLAVE_EVENT:
		return "SLAVE_EVENT"
	case CREATE_FILE_EVENT:
		return "CREATE_FILE_EVENT"
	case APPEND_BLOCK_EVENT:
		return "APPEND_BLOCK_EVENT"
	case EXEC_LOAD_EVENT:
		return "EXEC_LOAD_EVENT"
	case DELETE_FILE_EVENT:
		return "DELETE_FILE_EVENT"
	case NEW_LOAD_EVENT:
		return "NEW_LOAD_EVENT"
	case RAND_EVENT:
		return "RAND_EVENT"
	case USER_VAR_EVENT:
		return "USER_VAR_EVENT"
	case FORMAT_DESCRIPTION_EVENT:
		return "FORMAT_DESCRIPTION_EVENT"
	case XID_EVENT:
		return "XID_EVENT"
	case BEGIN_LOAD_QUERY_EVENT:
		return "BEGIN_LOAD_QUERY_EVENT"
	case EXECUTE_LOAD_QUERY_EVENT:
		return "EXECUTE_LOAD_QUERY_EVENT"
	case TABLE_MAP_EVENT:
		return "TABLE_MAP_EVENT"
	case WRITE_ROWS_EVENTv0:
		return "WRITE_ROWS_EVENTv0"
	case UPDATE_ROWS_EVENTv0:
		return "UPDATE_ROWS_EVENTv0"
	case DELETE_ROWS_EVENTv0:
		return "DELETE_ROWS_EVENTv0"
	case WRITE_ROWS_EVENTv1:
		return "WRITE_ROWS_EVENTv1"
	case UPDATE_ROWS_EVENTv1:
		return "UPDATE_ROWS_EVENTv1"
	case DELETE_ROWS_EVENTv1:
		return "DELETE_ROWS_EVENTv1"
	case INCIDENT_EVENT:
		return "INCIDENT_EVENT"
	case HEARTBEAT_EVENT:
		return "HEARTBEAT_EVENT"
	case IGNORABLE_EVENT:
		return "IGNORABLE_EVENT"
	case ROWS_QUERY_EVENT:
		return "ROWS_QUERY_EVENT"
	case WRITE_ROWS_EVENTv2:
		return "WRITE_ROWS_EVENTv2"
	case UPDATE_ROWS_EVENTv2:
		return "UPDATE_ROWS_EVENTv2"
	case DELETE_ROWS_EVENTv2:
		return "DELETE_ROWS_EVENTv2"
	case GTID_EVENT:
		return "GTID_EVENT"
	case ANONYMOUS_GTID_EVENT:
		return "ANONYMOUS_GTID_EVENT"
	case PREVIOUS_GTIDS_EVENT:
		return "PREVIOUS_GTIDS_EVENT"
	}
	return fmt.Sprintf("%d", header.EventType)
}

func (header *EventHeader) FlagNames() (names []string) {
	if header.Flags&LOG_EVENT_BINLOG_IN_USE_F != 0 {
		names = append(names, "LOG_EVENT_BINLOG_IN_USE_F")
	}
	if header.Flags&LOG_EVENT_FORCED_ROTATE_F != 0 {
		names = append(names, "LOG_EVENT_FORCED_ROTATE_F")
	}
	if header.Flags&LOG_EVENT_THREAD_SPECIFIC_F != 0 {
		names = append(names, "LOG_EVENT_THREAD_SPECIFIC_F")
	}
	if header.Flags&LOG_EVENT_SUPPRESS_USE_F != 0 {
		names = append(names, "LOG_EVENT_SUPPRESS_USE_F")
	}
	if header.Flags&LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F != 0 {
		names = append(names, "LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F")
	}
	if header.Flags&LOG_EVENT_ARTIFICIAL_F != 0 {
		names = append(names, "LOG_EVENT_ARTIFICIAL_F")
	}
	if header.Flags&LOG_EVENT_RELAY_LOG_F != 0 {
		names = append(names, "LOG_EVENT_RELAY_LOG_F")
	}
	if header.Flags&LOG_EVENT_IGNORABLE_F != 0 {
		names = append(names, "LOG_EVENT_IGNORABLE_F")
	}
	if header.Flags&LOG_EVENT_NO_FILTER_F != 0 {
		names = append(names, "LOG_EVENT_NO_FILTER_F")
	}
	if header.Flags&LOG_EVENT_MTS_ISOLATE_F != 0 {
		names = append(names, "LOG_EVENT_MTS_ISOLATE_F")
	}
	if header.Flags & ^(LOG_EVENT_MTS_ISOLATE_F<<1-1) != 0 { // unknown flags
		names = append(names, string(header.Flags & ^(LOG_EVENT_MTS_ISOLATE_F<<1-1)))
	}
	return names
}
