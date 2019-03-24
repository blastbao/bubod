//同步信息
package lib
import(
	"os"
	"io"
	"log"
	"fmt"
	"time"
)

// 每秒同步一次 file.pos
func (dumpConfig *DumpConfig) InstantSync() {
	for {
		newPos := fmt.Sprintf("%s:%d", dumpConfig.BinlogDumpFileName, dumpConfig.BinlogDumpPosition)
		if newPos != dumpConfig.SyncPos {
			dumpConfig.SyncBinlogFilenamePos(newPos)
		}
		log.Println("=============:",newPos)
		time.Sleep(1 * time.Second)
	}
}

// 保存当前 file.pos 信息到本地文件+zk
func (dumpConfig *DumpConfig) SyncBinlogFilenamePos(fileNamePos string) error {
	//检查字符串合法性
	if CheckBinlogFilePos(fileNamePos) == false {
		return fmt.Errorf("[error] SyncBinlogFilenamePos Invalid fileNamePos error %s", fileNamePos)
	}
	//打开本地文件并写入
	bubod_dump_pos := dumpConfig.Conf["Bubod"]["bubod_dump_pos"]
	f, err := os.OpenFile(bubod_dump_pos, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777) //打开文件
	if err !=nil {
		log.Println("[error] Write bubod_dump_pos OpenFile error:", bubod_dump_pos, "; Error:",err)
		return err
	}else{
		_, err := io.WriteString(f, fileNamePos)
		if err !=nil {
			log.Println("[error] Write bubod_dump_pos WriteString err:", bubod_dump_pos, "; Error:",err)
			return err
		}
	}	
	defer f.Close()
	//同步到zk
	if (dumpConfig.Conf["Zookeeper"]["server"] != ""){
		dumpConfig.ElectionManager.SetData(fileNamePos)
	}
	return nil
}