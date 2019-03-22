// documentation:
// https://dev.mysql.com/doc/internals/en/table-map-event.html
// The TABLE_MAP_EVENT defines the structure if the tables that are about to be changed.
// 所变更表的定义
package mysql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Bitfield []byte

func (bits Bitfield) isSet(index uint) bool {
	return bits[index/8]&(1<<(index%8)) != 0
}

//字段类型元数据
type ColumnType struct {
	column_type  FieldType  //字段类型
	name         string 	//字段名
	unsigned     bool 		//有无符号
	max_length   uint16     //变长类型字段，有最大长度约束
	length_size  uint8 		//变长数据长度
	precision    int 		//精度
	decimals     int        //小数
	size         uint16     //对于enum和set类型，size保存了当前列的数值用几个字节来存储
	bytes        int        //对于 bit 类型， bytes 保存了当前bit类型所需占用的字节数，注意 bytes = (bits + 7) / 8
	bits         byte       //对于 bit 类型， bits 保存了当前bit类型所需占用的二进制位数
	fsp          uint8      //时间戳
}

type TableMapEvent struct {
	header         EventHeader
	tableId        uint64           //
	flags          uint16          	//
	schemaName     string           //
	tableName      string 			//
	columnTypes    []FieldType      //
	columnMeta     []uint16 		//
	columnMetaData []*ColumnType    //
	nullBitmap     Bitfield         //
}


func (event *TableMapEvent) columnTypeNames() (names []string) {
	names = make([]string, len(event.columnTypes))
	for i, t := range event.columnTypes {
		names[i] = fieldTypeName(t)
	}
	return
}

func (event *TableMapEvent) parseColumnMetadata(data []byte) error {
	//event.columnMeta = make([]uint16, len(event.columnTypes))

	//字节偏移
	pos := 0
	//每个字段的元数据的长度取决于列字段类型，因此逐字段顺序解析。
	for i, t := range event.columnMetaData {
		switch t.column_type {
			
		case FIELD_TYPE_STRING:	//2B
			var b, c uint8
			b = uint8(data[pos])
			pos += 1
			c = uint8(data[pos])
			pos += 1
			metadata := (b << 8) + c
			if FieldType(b) == FIELD_TYPE_ENUM || FieldType(b) == FIELD_TYPE_SET {
				event.columnMetaData[i].column_type = FieldType(b)
				event.columnMetaData[i].size = uint16(metadata) & 0x00ff
			} else {
				event.columnMetaData[i].max_length = (((uint16(metadata) >> 4) & 0x300) ^ 0x300) + (uint16(metadata) & 0x00ff)
			}
		case FIELD_TYPE_VARCHAR, 
			 FIELD_TYPE_VAR_STRING, 
			 FIELD_TYPE_DECIMAL: //2B
			event.columnMetaData[i].max_length = bytesToUint16(data[pos : pos+2])
			pos += 2
		case FIELD_TYPE_BLOB,
			 FIELD_TYPE_GEOMETRY,
			 FIELD_TYPE_DOUBLE,
			 FIELD_TYPE_FLOAT,
		 	 FIELD_TYPE_TINY_BLOB,
			 FIELD_TYPE_MEDIUM_BLOB,
			 FIELD_TYPE_LONG_BLOB:
			event.columnMetaData[i].length_size = uint8(data[pos])
			pos += 1

		case FIELD_TYPE_NEWDECIMAL:
			event.columnMetaData[i].precision = int(data[pos])
			pos += 1
			event.columnMetaData[i].decimals = int(data[pos])
			pos += 1

		case FIELD_TYPE_BIT:
			bits := uint8(data[pos])
			pos += 1
			bytes := uint8(data[pos])
			pos += 1
			event.columnMetaData[i].bits = (bytes * 8) + bits
			event.columnMetaData[i].bytes = int((event.columnMetaData[i].bits + 7) / 8)

		case FIELD_TYPE_TIMESTAMP2, FIELD_TYPE_DATETIME2, FIELD_TYPE_TIME2:
			event.columnMetaData[i].fsp = uint8(data[pos])
			pos += 1

		case
			FIELD_TYPE_DATE,
			FIELD_TYPE_DATETIME,
			FIELD_TYPE_TIMESTAMP,
			FIELD_TYPE_TIME,
			FIELD_TYPE_TINY,
			FIELD_TYPE_SHORT,
			FIELD_TYPE_INT24,
			FIELD_TYPE_LONG,
			FIELD_TYPE_LONGLONG,
			FIELD_TYPE_NULL,
			FIELD_TYPE_YEAR,
			FIELD_TYPE_NEWDATE:
			event.columnMetaData[i].max_length = 0

		default:
			return fmt.Errorf("Unknown FieldType %d", t)
		}
	}
	return nil
}



//https://dev.mysql.com/doc/internals/en/table-map-event.html
// 
// post-header:
//     if post_header_len == 6 {
//   		4              table id
//     } else {
//   		6              table id
//     }
//   		2              flags
// payload:
//   		1              schema name length
//   		string         schema name
//   		1              [00]
//   		1              table name length
//   		string         table name
//   		1              [00]
//  		lenenc-int     column-count
//   		string.var_len [length=$column-count] column-def
//   		lenenc-str     column-meta-def
//   		n              NULL-bitmask, length: (column-count + 8) / 7
//
func (parser *eventParser) parseTableMapEvent(buf *bytes.Buffer) (event *TableMapEvent, err error) {
	var byteLength byte
	var columnCount, variableLength uint64

	//通用事件头 EventHeader
	event = new(TableMapEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)

	//获取 event.header.EventType 事件对应的私有事件头的长度
	headerSize := parser.format.eventTypeHeaderLengths[event.header.EventType-1]

	//根据 headerSize 字节数确定 tableIdSize 字节数
	var tableIdSize int
	if headerSize == 6 {
		tableIdSize = 4
	} else {
		tableIdSize = 6
	}

	//TableId: 4B or 6B
	event.tableId, err = readFixedLengthInteger(buf, tableIdSize)
	//Flags: 2B
	err = binary.Read(buf, binary.LittleEndian, &event.flags)

	//schema name length: 1B
	byteLength, err = buf.ReadByte()
	//schema name
	event.schemaName = string(buf.Next(int(byteLength)))
	//[00]: 1B
	_, err = buf.ReadByte()

	//table name length
	byteLength, err = buf.ReadByte()
	//table name
	event.tableName = string(buf.Next(int(byteLength)))
	//[00]: 1B
	_, err = buf.ReadByte()

	//列数目。
	columnCount, _, err = readLengthEncodedInt(buf)
	//列类型数组。它以长度编码字符串的形式发送，字符串中的每个字节代表列的类型 Protocol::ColumnType，字符串长度和列数目相等。
	event.columnTypes = make([]FieldType, columnCount)
	//列元数据数组。
	event.columnMetaData = make([]*ColumnType, columnCount)
	//读取 columnCount 个字节，每个字节 b 代表列的类型 Protocol::ColumnType
	columnData := buf.Next(int(columnCount))
	for i, b := range columnData {
		event.columnMetaData[i] = &ColumnType{column_type: FieldType(b)}
		event.columnTypes[i] = FieldType(b)
	}

	//column-meta-def (lenenc_str) -- array of metainfo per column, length is the overall length of the metainfo-array in bytes, 
	//the length of each metainfo field is dependent on the columns field type
	//解析列元数据，填充上面的 event.columnMetaData[]。
	variableLength, _, err = readLengthEncodedInt(buf)
	if err = event.parseColumnMetadata(buf.Next(int(variableLength))); err != nil {
		return
	}

	if buf.Len() < int((columnCount+7)/8) {
		err = io.EOF
	}

	//null_bitmap (string.var_len) -- [len=(column_count + 8) / 7]
	event.nullBitmap = Bitfield(buf.Next(int((columnCount + 7) / 8)))

	//Checksum: 4B
	if parser.binlog_checksum {
		buf.Next(4)
	}

	return
}