// https://dev.mysql.com/doc/internals/en/rotate-event.html
// binlog文件切换事件
package mysql

import (
	"bytes"
	"encoding/binary"
)


// 当binlog的文件大小超过阈值时，这个事件会写到binlog的结尾处，指向下一个binlog文件序列。
// 这个事件可以让slave知道下一个binlog文件的名字，以便它去接收。

// ROTATE_EVENT 是在 master 本地产生并写入 binlog 中的。
// 当 FLUSH LOGS 命令执行时或收到 master 的 ROTATE_EVENT 时，slave 会写到他的 relay log 中。


type RotateEvent struct {
	header   EventHeader
	position uint64 	//8字节。下个日志文件的第一个事件的位置。经常包含数字4（表示下个binlog的下个事件从位置4开始）。这个字段在v1中不存在。可以推测，这个值是4。
	filename string 	//下个binlog文件的名字。文件名不是以null结尾的。
}

func  (parser *eventParser) parseRotateEvent(buf *bytes.Buffer) (event *RotateEvent, err error) {
	event = new(RotateEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)
	err = binary.Read(buf, binary.LittleEndian, &event.position)
	event.filename = buf.String()
	return
}