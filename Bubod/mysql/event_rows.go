// documentation:
// https://dev.mysql.com/doc/internals/en/rows-event.html
// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
// 数据变更事件
package mysql

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"time"
	"log"
	// "encoding/json"
)

// ROWS_EVENT 简介: 
//
// 对于ROW格式的binlog，所有的DML语句都是记录在ROWS_EVENT中。
// ROWS_EVENT分为三种：WRITE_ROWS_EVENT，UPDATE_ROWS_EVENT，DELETE_ROWS_EVENT，分别对应 insert，update 和 delete 操作。
//
// 对于insert操作，WRITE_ROWS_EVENT包含了要插入的数据；
// 对于update操作，UPDATE_ROWS_EVENT不仅包含了修改后的数据，还包含了修改前的值；
// 对于delete操作，仅仅需要指定删除的主键（在没有主键的情况下，会给定所有列）。
// 
// 注意，对于 QUERY_EVENT 事件，是以文本形式记录DML操作的。而对于ROWS_EVENT事件，并不是文本形式，
// 所以在通过 mysqlbinlog 查看基于 ROW 格式的 binlog 时，需要指定 -vv --base64-output=decode-rows。

type RowsEvent struct {
	header                EventHeader
	tableId               uint64            // 表id
	tableMap              *TableMapEvent    // 
	flags                 uint16
	columnsPresentBitmap1 Bitfield
	columnsPresentBitmap2 Bitfield
	rows                  []map[string]driver.Value // 记录了所有的变更行
	primary			  	  string					// 记录主键字段
	// ColumnSchemaType	  *column_schema_type 		// 表字段属性
}

//reference: https://dev.mysql.com/doc/internals/en/rows-event.html
//
// header:
//
//   if post_header_len == 6 {
// 		4                    table id
//   } else {
// 		6                    table id
//   }
// 		2                    flags
//  
//   if version == 2 {
// 		2                    extra-data-length
// 		string.var_len       extra-data
//   }
//
// body:
// 		lenenc_int           number of columns
// 		string.var_len       columns-present-bitmap1, length: (num of columns+7)/8
//   if UPDATE_ROWS_EVENTv1 or v2 {
// 		string.var_len       columns-present-bitmap2, length: (num of columns+7)/8
//   }
//
// rows:
// 		string.var_len       nul-bitmap, length (bits set in 'columns-present-bitmap1'+7)/8
// 		string.var_len       value of each field as defined in table-map
//   if UPDATE_ROWS_EVENTv1 or v2 {
// 		string.var_len       nul-bitmap, length (bits set in 'columns-present-bitmap2'+7)/8
// 		string.var_len       value of each field as defined in table-map
//   }
//   ... repeat rows until event-end

func (parser *eventParser) parseRowsEvent(buf *bytes.Buffer) (event *RowsEvent, err error) {
	var columnCount uint64

	//通用事件头 EventHeader
	event = new(RowsEvent)
	err = binary.Read(buf, binary.LittleEndian, &event.header)

	//获取 event.header.EventType 事件对应的私有事件头的长度
	headerSize := parser.format.eventTypeHeaderLengths[event.header.EventType-1]
	var tableIdSize int
	if headerSize == 6 {
		tableIdSize = 4
	} else {
		tableIdSize = 6
	}

	// event = &RowsEvent{}
	// if parser.format.EventTypeHeaderLengths[h.EventType-1] == 6 {
	// 		event.tableIDSize = 4
	// } else {
	// 		event.tableIDSize = 6
	// }

	//TableId: 4B or 6B, 如果 TableId 是 0x00ffffff，则它是一个伪事件，它应该设置语句结束标志，声明可以释放所有表映射。
	event.tableId, err = readFixedLengthInteger(buf, tableIdSize)

	// log.Println("======parser.tableMap:", parser.tableMap)
	// log.Println("======parser.tableSchemaMap:", parser.tableSchemaMap)
	// log.Println("======event.tableId:", event.tableId)
	// log.Println("======SchemaMap:", parser.tableSchemaMap[event.tableId])
	
	// for i,w := range parser.tableSchemaMap {
	// 	log.Println("======:", i)
	// 	for _, w0 := range w {
	// 		log.Println("	==:",  w0)
	// 	}
	// }


	//Flags: 2B, 
	// 0x0001 - end of statement, 
	// 0x0002 - no foreign key checks, 
	// 0x0004 - no unique key checks,
	// 0x0008 - row has a columns
	err = binary.Read(buf, binary.LittleEndian, &event.flags)

	switch event.header.EventType {
	case UPDATE_ROWS_EVENTv2, WRITE_ROWS_EVENTv2, DELETE_ROWS_EVENTv2: // written from MySQL 5.6.x, added the extra-data fields
		//err = binary.Read(buf, binary.LittleEndian, &event.flags)
		//extra_data_len: 2B, length of extra_data (has to be ≥ 2)
		extraDataLength,_:=readFixedLengthInteger(buf, 2)
		//extra_data: ignore
		buf.Next(int(extraDataLength/8))
		break
	}

	//列数目。
	columnCount, _, err = readLengthEncodedInt(buf)
	// columns-present-bitmap1, length: (num of columns+7)/8
	event.columnsPresentBitmap1 = Bitfield(buf.Next(int((columnCount + 7) / 8)))
	switch event.header.EventType {
	case UPDATE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv2:
		//columns-present-bitmap2, length: (num of columns+7)/8
		event.columnsPresentBitmap2 = Bitfield(buf.Next(int((columnCount + 7) / 8)))
	}
	

	event.tableMap = parser.tableMap[event.tableId]
	for buf.Len() > 0 {

		// 从 buf 中解析出一个 RowEvent，转成 map[field_name][field_value] 格式
		var row map[string]driver.Value
		row, err = parser.parseEventRow(buf, event.tableMap, parser.tableSchemaMap[event.tableId])
		if err != nil {
			log.Println("event row parser err:",err)
			return
		}

		// 保存
		event.rows = append(event.rows, row)

		// 判断设置的 COLUMN_KEY 约束类型，来获取主键字段
		for _, w := range parser.tableSchemaMap[event.tableId]{
			// b, err := json.Marshal(w)
			// if err != nil {
			// 	fmt.Println("encoding faild")
			// } else {
			// 	log.Println("================:", string(b), w.auto_increment, "##")
			// }
			if w.is_primary == true && w.COLUMN_KEY == "PRI" {
				event.primary = w.COLUMN_NAME
			}
		}
	}
	return
}

// 字段=值，值类型转换
func (parser *eventParser) parseEventRow(buf *bytes.Buffer, tableMap *TableMapEvent, tableSchemaMap []*column_schema_type) (row map[string]driver.Value, e error) {
	columnsCount := len(tableMap.columnTypes)
	bitfieldSize := (columnsCount + 7) / 8

	nullBitMap := Bitfield(buf.Next(bitfieldSize))  		// 空字段位图，若第i字段值为null，就设置nullBitMap的第i位为1，以节省存储
	

	row = make(map[string]driver.Value)



	for i := 0; i < columnsCount; i++ { 					// 逐列遍历字段 Meta 信息表，它是按照表字段名升序排序的。
		column_name := tableSchemaMap[i].COLUMN_NAME      	// 字段名
		if nullBitMap.isSet(uint(i)) {                      // 将空字段置为nil
			row[column_name] = nil
			continue
		}

		switch tableMap.columnMetaData[i].column_type {     // 为啥是 tableMap.columnMetaData[i].column_type 而不是 tableSchemaMap[i].COLUMN_TYPE ? 
		case FIELD_TYPE_NULL: //null
			row[column_name] = nil

		case FIELD_TYPE_TINY: //bool or uint8 or int8
			var b byte
			b, e = buf.ReadByte()
			if tableSchemaMap[i].is_bool == true{
				switch int(b) {
				case 1:row[column_name] = true
				case 0:row[column_name] = false
				default:
					if tableSchemaMap[i].unsigned == true{
						row[column_name] = uint8(b)
					}else{
						row[column_name] = int8(b)
					}
				}
			}else{
				if tableSchemaMap[i].unsigned == true{
					row[column_name] = uint8(b)
				}else{
					row[column_name] = int8(b)
				}
			}

		case FIELD_TYPE_SHORT: //uint16 or int16
			if tableSchemaMap[i].unsigned{
				var short uint16
				e = binary.Read(buf, binary.LittleEndian, &short)
				row[column_name] = short
			}else{
				var short int16
				e = binary.Read(buf, binary.LittleEndian, &short)
				row[column_name] = short
			}

		case FIELD_TYPE_YEAR: //1900+x
			var b byte
			b, e = buf.ReadByte()
			if e == nil && b != 0 {
				//time.Date(int(b)+1900, time.January, 0, 0, 0, 0, 0, time.UTC)
				row[column_name] = strconv.Itoa(int(b) + 1900)
			}

		case FIELD_TYPE_INT24: //uint32 or int32
			if tableSchemaMap[i].unsigned {
				var bint uint64
				bint, e = readFixedLengthInteger(buf, 3)
				row[column_name] = uint32(bint)
			}else{
				var a,b,c int8
				binary.Read(buf,binary.LittleEndian,&a)
				binary.Read(buf,binary.LittleEndian,&b)
				binary.Read(buf,binary.LittleEndian,&c)
				row[column_name] = int32(a + (b << 8) + (c << 16))
			}

		case FIELD_TYPE_LONG: //uint32 or int32
			if tableSchemaMap[i].unsigned {
				var long uint32
				e = binary.Read(buf, binary.LittleEndian, &long)
				row[column_name] = long
			}else{
				var long int32
				e = binary.Read(buf, binary.LittleEndian, &long)
				row[column_name] = long
			}

		case FIELD_TYPE_LONGLONG: //uint64 or int64
			if tableSchemaMap[i].unsigned{
				var longlong uint64
				e = binary.Read(buf, binary.LittleEndian, &longlong)
				row[column_name] = longlong
			}else {
				var longlong int64
				e = binary.Read(buf, binary.LittleEndian, &longlong)
				row[column_name] = longlong
			}

		case FIELD_TYPE_FLOAT: //float32
			var float float32
			e = binary.Read(buf, binary.LittleEndian, &float)
			row[column_name] = float

		case FIELD_TYPE_DOUBLE: //float64
			var double float64
			e = binary.Read(buf, binary.LittleEndian, &double)
			row[column_name] = double

		case FIELD_TYPE_DECIMAL: //...
			return nil, fmt.Errorf("parseEventRow unimplemented for field type %s", fieldTypeName(tableMap.columnTypes[i]))

		case FIELD_TYPE_NEWDECIMAL: //...
			digits_per_integer := 9
			compressed_bytes := [10]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}
			integral := (tableMap.columnMetaData[i].precision - tableMap.columnMetaData[i].decimals)
			uncomp_integral := int(int(integral) / digits_per_integer)
			uncomp_fractional := int(int(tableMap.columnMetaData[i].decimals) / digits_per_integer)
			comp_integral := integral - (uncomp_integral * digits_per_integer)
			comp_fractional := tableMap.columnMetaData[i].decimals - (uncomp_fractional * digits_per_integer)

			var value int
			var res string
			var mask int
			var size int
			size = compressed_bytes[comp_integral]

			bufPaket := &paket{
				buf:     buf,
				buydata: make([]byte, 0),
			}
			b := bufPaket.readByte()

			if int(b)&128 != 0 {
				res = ""
				mask = 0
			} else {
				mask = -1
				res = "-"
			}

			var tmp *bytes.Buffer = new(bytes.Buffer)
			binary.Write(tmp, binary.LittleEndian, uint8(b)^128)
			bufPaket.unread(tmp.Next(1))

			if size > 0 {
				d := bufPaket.read(size)
				data := bytes.NewBuffer(d)

				var v1 int32
				binary.Read(data, binary.BigEndian, &v1)
				value = int(v1) ^ mask
				res += strconv.Itoa(value)
			}

			for i := 0; i < uncomp_integral; i++ {
				value = int(read_uint64_be_by_bytes(bufPaket.read(4))) ^ mask
				res += fmt.Sprintf("%09d", value)
			}

			res += "."

			for i := 0; i < uncomp_integral; i++ {
				value = int(read_uint64_be_by_bytes(bufPaket.read(4))) ^ mask
				res += fmt.Sprintf("%09d", value)
			}
			size = compressed_bytes[comp_fractional]
			if size > 0 {
				value = int(read_uint64_be_by_bytes(bufPaket.read(size))) ^ mask
				res += fmt.Sprintf("%0*d", comp_fractional, value)
			}
			row[column_name] = res

		case FIELD_TYPE_VARCHAR: //string
			max_length := tableMap.columnMetaData[i].max_length
			var length int
			//对于varchar类型，如果最大长度超过255，会用两个字节保存长度
			if max_length > 255 {
				var short uint16
				e = binary.Read(buf, binary.LittleEndian, &short)
				length = int(short)
			} else {
				var b byte
				b, e = buf.ReadByte()
				length = int(b)
			}
			//数据不足
			if buf.Len() < length {
				e = io.EOF
			}
			row[column_name] = string(buf.Next(length))

		case FIELD_TYPE_STRING:
			var length int
			var b byte
			b, e = buf.ReadByte()
			length = int(b)
			row[column_name] = string(buf.Next(length))

		case FIELD_TYPE_ENUM:
			//对于enum和set类型，size保存了当前列的数值用几个字节来存储
			size := tableMap.columnMetaData[i].size
			var index int
			if size == 1 {
				var b byte
				b, _ = buf.ReadByte()
				index = int(b)
			} else {
				index = int(bytesToUint16(buf.Next(int(size))))
			}
			//反查enum_values[]表获取枚举对应的真实值
			row[column_name] = tableSchemaMap[i].enum_values[index-1] 

		case FIELD_TYPE_SET:
			//对于enum和set类型，size保存了当前列的值用几个字节来存储
			size := tableMap.columnMetaData[i].size
			var index int
			switch size {
			case 0:
				row[column_name] = nil
				break
			case 1:
				var b byte
				b, _ = buf.ReadByte()
				index = int(b)
			case 2:
				index = int(bytesToUint16(buf.Next(int(size))))
			case 3:
				index = int(bytesToUint24(buf.Next(int(size))))
			case 4:
				index = int(bytesToUint32(buf.Next(int(size))))
			default:
				index = 0
			}
			result := make(map[string]int, 0)
			// mathPower(2,i) 相当于将 0x01 左移 n 位得到的新的数值
			var mathPower = func (x int, n int) int {
								ans := 1
								for n != 0 {
									ans *= x  //ans *= x 相当于x进制表示的左移1位
									n--       //n表示最多左移动几次
								}
								return ans
							}
			// 相当于 bitmap & mask，获取有效位 i 对应的值 set_values[i]
			for i, val := range tableSchemaMap[i].set_values {
				s := index & mathPower(2,i) 
				if s > 0 {
					result[val] = 1
				}
			}

			f := make([]string, 0)
			for key, _ := range result {
				f = append(f, key)
			}
			row[column_name] = f

		case FIELD_TYPE_BLOB,
			 FIELD_TYPE_TINY_BLOB, 
			 FIELD_TYPE_MEDIUM_BLOB,
			 FIELD_TYPE_LONG_BLOB, 
			 FIELD_TYPE_VAR_STRING:
			var length uint64
			length, e = readFixedLengthInteger(buf, int(tableMap.columnMetaData[i].length_size))
			row[column_name] = string(buf.Next(int(length)))
			break

		case FIELD_TYPE_BIT:

			var resp string = ""

			//对于bit类型，bytes保存了当前bit类型需占用的字节数。
			for k := 0; k < tableMap.columnMetaData[i].bytes; k++ {
				//var current_byte = ""
				var current_byte []string
				var b byte
				var end byte
				var data int

				//每次循环读取一个字节
				b, e = buf.ReadByte()
				data = int(b)


				//注意，二进制序列是从高位到低位逐个字节读取的，bitmap总共占n个字节，
				//实际上 total bits = (n-1)*bytes + some residual bits，
				//这些残余的bits都位于高位字节，所以对于最高位的字节，需要判断它是否
				//只包含了部分有效的 residual bits。

				// 第一个高位字节
				if k == 0 {
					// 如果本字段只占一个字节，那么只需遍历 bits 个位
					if tableMap.columnMetaData[i].bytes == 1 {
						end = tableMap.columnMetaData[i].bits
					} else {
						//模8后的结果 end 即本字节内的有效位数，若模8值为0，则整个字节都有效。
						end = tableMap.columnMetaData[i].bits % 8
						if end == 0 {
							end = 8
						}
					}
				} else {
					end = 8
				}

				//将字节表示成二进制的字符串表示，注意遍历顺序是从低位到高位，所以通过append添加得到
				//字符串为: byte(00001111) => "11110000"。
				var bit uint
				for bit = 0; bit < uint(end); bit++ {
					tmp := 1 << bit
					if (data & tmp) > 0 {
						current_byte = append(current_byte, "1")
					} else {
						current_byte = append(current_byte, "0")
					}
				}

				// 把字节的字符串表示逆序修正后，追加到resp中。
				for k := len(current_byte); k > 0; k-- {
					resp += current_byte[k-1]
				}
			}

			bitInt, _ := strconv.ParseInt(resp, 2, 10)  // 以二进制方式将字符串resp解码成10位的整数
			row[column_name] = bitInt
			break

		case FIELD_TYPE_GEOMETRY:
			return nil, fmt.Errorf("parseEventRow unimplemented for field type %s", fieldTypeName(tableMap.columnTypes[i]))

		case FIELD_TYPE_DATE, FIELD_TYPE_NEWDATE:
			var data []byte
			data = buf.Next(3)
			timeInt := int(int(data[0]) + (int(data[1]) << 8) + (int(data[2]) << 16))
			if timeInt == 0 {
				row[column_name] = nil
			} else {
				year  := (timeInt & (((1 << 15) - 1) << 9)) >> 9
				month := (timeInt & (((1 << 4) - 1) << 5)) >> 5
				day   := (timeInt & ((1 << 5) - 1))
				var monthStr, dayStr string
				if month >= 10 {
					monthStr = strconv.Itoa(month)
				} else {
					monthStr = "0" + strconv.Itoa(month)
				}
				if day >= 10 {
					dayStr = strconv.Itoa(day)
				} else {
					dayStr = "0" + strconv.Itoa(day)
				}
				t := strconv.Itoa(year) + "-" + monthStr + "-" + dayStr
				///tm, _ := time.Parse("2006-01-02", t)
				row[column_name] = t
			}

		case FIELD_TYPE_TIME:
			var data []byte
			data = buf.Next(3)
			timeInt := int(int(data[0]) + (int(data[1]) << 8) + (int(data[2]) << 16))
			if timeInt == 0 {
				row[column_name] = nil
			} else {
				hour := int(timeInt / 10000)
				minute := int((timeInt % 10000) / 100)
				second := int(timeInt % 100)
				var minuteStr, secondStr string
				if minute > 10 {
					minuteStr = strconv.Itoa(minute)
				} else {
					minuteStr = "0" + strconv.Itoa(minute)
				}
				if second > 10 {
					secondStr = strconv.Itoa(second)
				} else {
					secondStr = "0" + strconv.Itoa(second)
				}
				t := strconv.Itoa(hour) + ":" + minuteStr + ":" + secondStr
				//tm, _ := time.Parse("15:04:05", t)
				//row[column_name] = tm.Format("15:04:05")
				row[column_name] = t
			}

		case FIELD_TYPE_TIME2:
			var a byte
			var b byte
			var c byte
			binary.Read(buf,binary.BigEndian,&a)
			binary.Read(buf,binary.BigEndian,&b)
			binary.Read(buf,binary.BigEndian,&c)
			timeInt := uint64((int(a) << 16) | (int(b) << 8) | int(c))
			if timeInt >= 0x800000{
				timeInt -= 0x1000000
			}
			hour := read_binary_slice(timeInt,2,10,24)
			minute := read_binary_slice(timeInt,12,6,24)
			second := read_binary_slice(timeInt,18,6,24)

			var minuteStr, secondStr string
			if minute > 10 {
				minuteStr = strconv.Itoa(int(minute))
			} else {
				minuteStr = "0" + strconv.Itoa(int(minute))
			}
			if second > 10 {
				secondStr = strconv.Itoa(int(second))
			} else {
				secondStr = "0" + strconv.Itoa(int(second))
			}
			t := strconv.Itoa(int(hour)) + ":" + minuteStr + ":" + secondStr
			row[column_name] = t
			break

		case FIELD_TYPE_TIMESTAMP:
			timestamp := int64(bytesToUint32(buf.Next(4)))
			tm := time.Unix(timestamp, 0)
			row[column_name] = tm.Format(TIME_FORMAT)
			break

		case FIELD_TYPE_TIMESTAMP2:
			var timestamp int32
			binary.Read(buf,binary.BigEndian,&timestamp)
			tm := time.Unix(int64(timestamp), 0)
			row[column_name] = tm.Format(TIME_FORMAT)
			break

		case FIELD_TYPE_DATETIME:
			var t int64
			e = binary.Read(buf, binary.LittleEndian, &t)

			second := int(t % 100)
			minute := int((t % 10000) / 100)
			hour   := int((t % 1000000) / 10000)
			d      := int(t / 1000000)
			day    := d % 100
			month  := time.Month((d % 10000) / 100)
			year   := d / 10000

			row[column_name] = time.Date(year, month, day, hour, minute, second, 0, time.UTC).Format(TIME_FORMAT)
			break

		case FIELD_TYPE_DATETIME2:
			row[column_name],e = read_datetime2(buf)
			break

		default:
			return nil, fmt.Errorf("Unknown FieldType %d", tableMap.columnTypes[i])
		}
		if e != nil {
			log.Println("lastFiled err:",tableMap.columnMetaData[i].column_type,e)
			return nil, e
		}
	}
	return
}

/*
Read a part of binary data and extract a number.

Params:
	binary: the data
	start: From which bit (1 to X)
	size: How many bits should be read
	data_length: data size
*/
func read_binary_slice(binary uint64, start uint64, size uint64, data_length uint64) uint64 {
	binary = binary >> (data_length - (start + size))
	mask := (1 << size) - 1
	return binary & uint64(mask)
}


/*
DATETIME

1  bit  sign           (1= non-negative, 0= negative)
17 bits year*13+month  (year 0-9999, month 0-12)
5  bits day            (0-31)
5  bits hour           (0-23)
6  bits minute         (0-59)
6  bits second         (0-59)
---------------------------
40 bits = 5 bytes
*/
func read_datetime2(buf *bytes.Buffer)(data string,err error) {
	defer func(){
		if errs:=recover();errs!=nil{
			err = fmt.Errorf(fmt.Sprint(errs))
			return
		}
	}()
	var b byte
	var a uint32
	binary.Read(buf, binary.BigEndian, &a)
	binary.Read(buf, binary.BigEndian, &b)
	/*
	log.Println("read_datetime2 a:",a)
	log.Println("read_datetime2 b:",b)
	log.Println("a << 8:",uint(a) << 8)
	*/
	dataInt 	:= uint64(b) + uint64((uint(a) << 8))
	year_month 	:= read_binary_slice(dataInt, 1, 17, 40)
	year 		:= int(year_month / 13)
	month 		:= time.Month(year_month % 13)
	days 		:= read_binary_slice(dataInt, 18, 5, 40)
	hours		:= read_binary_slice(dataInt, 23, 5, 40)
	minute 		:=read_binary_slice(dataInt, 28, 6, 40)
	second 		:=read_binary_slice(dataInt, 34, 6, 40)
	data = time.Date(year, month, int(days), int(hours), int(minute), int(second), 0, time.UTC).Format(TIME_FORMAT)
	return
}