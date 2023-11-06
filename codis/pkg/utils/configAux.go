package utils

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"

	"pika/codis/v2/pkg/utils/bytesize"
	"pika/codis/v2/pkg/utils/errors"
	"pika/codis/v2/pkg/utils/log"
	"pika/codis/v2/pkg/utils/timesize"
)

const (
	TypeConf = iota
	TypeComment
)

type ConfItem struct {
	confType int // 0 means conf, 1 means comment
	name     string
	value    string
}

type DeployConfig struct {
	items   []*ConfItem
	confMap map[string]*ConfItem
	sep     string //配置项中key、value分隔符
}

func (c *DeployConfig) Init(path string, sep string) error {
	c.confMap = make(map[string]*ConfItem)
	c.sep = sep

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		b, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// 拿到一行并去除两边空白字符
		line := strings.TrimSpace(string(b))

		item := &ConfItem{}
		//第一个字符为“#”或者空行都认为是注释
		if strings.Index(line, "#") == 0 || len(line) == 0 {
			item.confType = TypeComment
			item.name = line
			c.items = append(c.items, item)
			continue
		}
		//找不到 = ，说明这行配置有问题
		index := strings.Index(line, sep)
		if index <= 0 {
			continue
		}
		//key不可以为空，value可以为空
		key := strings.TrimSpace(line[:index])
		value := strings.TrimSpace(line[index+1:])
		if len(key) == 0 {
			continue
		}
		item.confType = TypeConf
		item.name = key
		item.value = value
		c.items = append(c.items, item)
		c.confMap[item.name] = item
	}
}

func (c *DeployConfig) Reset(conf interface{}, isWrap bool) {
	obj := reflect.ValueOf(conf)

	for i := 0; i < obj.NumField(); i++ {
		fieldInfo := obj.Type().Field(i)
		name := fieldInfo.Tag.Get("toml")
		//  检查字段的toml tag是否合法
		if name == "" || name == "-" {
			continue
		}
		var value string
		switch v := obj.Field(i).Interface().(type) {
		case string:
			value = strings.Trim(strings.TrimSpace(v), "\"")
			if value == "" {
				continue
			}
			if isWrap {
				c.Set(name, "\""+value+"\"")
			} else {
				c.Set(name, value)
			}
		case int:
			value = strconv.Itoa(v)
			c.Set(name, value)
		case int32:
			value = strconv.FormatInt(int64(v), 10)
			c.Set(name, value)

		case int64:
			value = strconv.FormatInt(v, 10)
			c.Set(name, value)
		case bool:
			if v {
				c.Set(name, "true")
			} else {
				c.Set(name, "false")
			}
		case timesize.Duration:
			if ret, err := v.MarshalText(); err != nil {
				log.WarnErrorf(err, "config set %s failed.\n", name)
			} else {
				value = string(ret[:])
				c.Set(name, "\""+value+"\"")
			}

		case bytesize.Int64:
			if ret, err := v.MarshalText(); err != nil {
				log.WarnErrorf(err, "config set %s failed.\n", name)
			} else {
				value = string(ret[:])
				c.Set(name, "\""+value+"\"")
			}

		default:
			log.Warnf("value error: %v\n", v)
			continue
		}
	}

}

func (c *DeployConfig) Set(key string, value string) error {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)

	log.Infof("Set key : %s, value: %s\n", key, value)

	if len(key) == 0 || len(value) == 0 {
		return errors.New("key or value is null")
	}

	item, found := c.confMap[key]
	if found {
		item.value = value
	} else {
		item := &ConfItem{}
		item.confType = TypeConf
		item.name = key
		item.value = value
		c.items = append(c.items, item)
		c.confMap[item.name] = item
	}
	return nil
}

func (c *DeployConfig) Get(key string) string {
	item, found := c.confMap[key]
	if !found {
		return ""
	}
	return item.value
}

func (c *DeployConfig) Show() {
	log.Infof("Show config, len = %d\n", len(c.items))
	for index, item := range c.items {
		if item.confType == TypeComment {
			//  注释的格式:  id: context
			log.Infof("%d: %s\n", index, item.name)
		} else {
			//   配置文件的格式: id: key = value  或者  id: key value
			if len(strings.TrimSpace(c.sep)) > 0 {
				log.Infof("%d: %s %s %s\n", index, item.name, c.sep, item.value)
			} else {
				log.Infof("%d: %s%s%s\n", index, item.name, c.sep, item.value)
			}
		}
	}
}

func (c *DeployConfig) ReWrite(confName string) error {
	//  confName = DefaultName.tmp
	f, err := os.Create(confName)
	if err != nil {
		log.WarnErrorf(err, "create %s failed.\n", confName)
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	for _, item := range c.items {
		var lineStr string
		if item.confType == TypeComment {
			lineStr = fmt.Sprintf("%s", item.name)
		} else {
			if len(strings.TrimSpace(c.sep)) > 0 {
				lineStr = fmt.Sprintf("%s %s %s", item.name, c.sep, item.value)
			} else {
				lineStr = fmt.Sprintf("%s%s%s", item.name, c.sep, item.value)
			}
		}
		fmt.Fprintln(w, lineStr)
	}
	return w.Flush()
}

func RewriteConfig(postConf interface{}, defaultConf string, sep string, isWrap bool) error {
	conf := new(DeployConfig)
	err := conf.Init(defaultConf, sep)
	if err != nil {
		log.WarnErrorf(err, "open  %s file failed.\n", defaultConf)
		return err
	}
	conf.Reset(postConf, isWrap)
	conf.Show()
	//  重写一份config
	var newConf = defaultConf + ".tmp"
	if err = conf.ReWrite(newConf); err != nil {
		return err
	} else {
		if err = os.Remove(defaultConf); err != nil {
			return err
		} else {
			return os.Rename(newConf, defaultConf)
		}
	}
}
