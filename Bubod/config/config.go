/*
Copyright [2018] [jc3wish]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package config

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

var MyConf map[string]map[string]string

func init() {
	MyConf = make(map[string]map[string]string)
}

// 加载配置文件
func LoadConf(conf_file string) map[string]map[string]string {
	per := make(map[string]map[string]string)
	
	f, _ := os.Open(conf_file)
    defer f.Close()

	buf := bufio.NewReader(f)
	stringKey := ""
	for {
		//逐行解析
		l, err := buf.ReadString('\n')
		line := strings.TrimSpace(l)
		if err != nil {
			if err != io.EOF {
				fmt.Println("config file isn't exsit or file is nothing!", err)
				os.Exit(1)
				// panic(err)
				break
			} else {
				break
			}
		}

		switch {
		case len(line) == 0:
		//[xxx]: 配置组名称
		case line[0] == '[' && line[len(line)-1] == ']':
			stringKey = strings.TrimSpace(line[1 : len(line)-1])
			per[stringKey] = make(map[string]string)
		case line[0] == '#':
		case line[0] == ';':
		default:
			//a=xxx: 配置项
			i := strings.IndexAny(line, "=")
			per[stringKey][strings.TrimSpace(line[0:i])] = strings.TrimSpace(line[i+1:])
		}
	}

	MyConf = per
	return MyConf
}

func GetConf(module string) map[string]string {
	return MyConf[module]
}

func GetConfigVal(module string, key string) string{
	if _,ok := MyConf[module];!ok{
		return ""
	}
	if _,ok := MyConf[module][key];!ok{
		return ""
	}
	return MyConf[module][key]
}

func SetConfigVal(module string, key string, value string) bool {
	if _,ok := MyConf[module];!ok{
		return false
	}
	if _,ok := MyConf[module][key];!ok{
		return false
	}
	MyConf[module][key] = value
	return true
}