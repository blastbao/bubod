// https://dev.mysql.com/doc/internals/en/format-description-event.html
// A format description event is the first event of a binlog for binlog-version 4. 
// It describes how the other events are layed out.
// 事件格式描述
// 格式描述事件是.binlog-version 4 的binlog的第一个事件。它描述了其他事件是如何被渲染出来的。
package mysql

import (
	"bytes"
	"encoding/binary"
)


// 简介:

// FORMAT_DESCRIPTION_EVENT 是最基础的 Event ，它是binlog文件中的第一个事件，而且，该事件只会在binlog中出现一次。
// FORMAT_DESCRIPTION_EVENT 通常指定了MySQL Server的版本，binlog的版本，该binlog文件的创建时间。
// MySQL根据 FORMAT_DESCRIPTION_EVENT 的定义来解析其它事件。

// 布局: FormatDescriptionEvent 事件的 EventHeader + EventData 结构布局如下
//
// 属性				字节数	含义
// binlogVersion	2		binlog版本
// serverVersion	50		服务器版本
// timestamp		4		该字段指明该binlog文件的创建时间。
// headerLength		1		事件头长度，为19
// headerArrays		n		一个数组，标识所有事件的私有事件头的长度，event 的个数定义 5.6，5.7为40个，也就是40个字节。
//
// 注意，这里的n可以算出，n = EventHeader.eventLength - sizeof(EventHeader) - sizeof(CheckSum) - (2 + 50 + 4 + 1)。

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



type FormatDescriptionEvent struct {
	header                 EventHeader
	binlogVersion          uint16
	mysqlServerVersion     string
	createTimestamp        uint32
	eventHeaderLength      uint8
	eventTypeHeaderLengths []byte
}

func (parser *eventParser) parseFormatDescriptionEvent(buf *bytes.Buffer) (event *FormatDescriptionEvent, err error) {
	event = new(FormatDescriptionEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	err = binary.Read(buf, binary.LittleEndian, &event.binlogVersion)
	event.mysqlServerVersion = string(buf.Next(50))
	err = binary.Read(buf, binary.LittleEndian, &event.createTimestamp)
	event.eventHeaderLength, err = buf.ReadByte()
	event.eventTypeHeaderLengths = buf.Bytes()
	return
}