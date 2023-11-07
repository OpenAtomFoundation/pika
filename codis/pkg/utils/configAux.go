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
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.WarnErrorf(err, "Close %s failed.\n", path)
		}
	}(f)

	r := bufio.NewReader(f)
	for {
		b, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		line := strings.TrimSpace(string(b))

		item := &ConfItem{}
		if strings.Index(line, "#") == 0 || len(line) == 0 {
			item.confType = TypeComment
			item.name = line
			c.items = append(c.items, item)
			continue
		}
		index := strings.Index(line, sep)
		if index <= 0 {
			continue
		}
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
				err := c.Set(name, "\""+value+"\"")
				if err != nil {
					log.WarnErrorf(err, "Set string with wrap failed!")
				}
			} else {
				err := c.Set(name, value)
				if err != nil {
					log.WarnErrorf(err, "Set string without wrap failed!")
				}
			}
		case int:
			value = strconv.Itoa(v)
			err := c.Set(name, value)
			if err != nil {
				log.WarnErrorf(err, "Set int failed!")
			}
		case int32:
			value = strconv.FormatInt(int64(v), 10)
			err := c.Set(name, value)
			if err != nil {
				log.WarnErrorf(err, "Set int32 failed!")
			}

		case int64:
			value = strconv.FormatInt(v, 10)
			err := c.Set(name, value)
			if err != nil {
				log.WarnErrorf(err, "Set int64 failed!")
			}
		case bool:
			if v {
				err := c.Set(name, "true")
				if err != nil {
					log.WarnErrorf(err, "Set bool value failed!")
				}
			} else {
				err := c.Set(name, "false")
				if err != nil {
					log.WarnErrorf(err, "Set bool value failed!")
				}
			}
		case timesize.Duration:
			if ret, err := v.MarshalText(); err != nil {
				log.WarnErrorf(err, "config set %s failed.\n", name)
			} else {
				value = string(ret[:])
				err := c.Set(name, "\""+value+"\"")
				if err != nil {
					log.WarnErrorf(err, "Set timesize failed!")
				}
			}

		case bytesize.Int64:
			if ret, err := v.MarshalText(); err != nil {
				log.WarnErrorf(err, "config set %s failed.\n", name)
			} else {
				value = string(ret[:])
				err := c.Set(name, "\""+value+"\"")
				if err != nil {
					log.WarnErrorf(err, "Set bytesize failed!")
				}
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
			//  Comment format:  id: context
			log.Infof("%d: %s\n", index, item.name)
		} else {
			//   Configuration format: id: key = value  or  id: key value
			if len(strings.TrimSpace(c.sep)) > 0 {
				log.Infof("%d: %s %s %s\n", index, item.name, c.sep, item.value)
			} else {
				log.Infof("%d: %s%s%s\n", index, item.name, c.sep, item.value)
			}
		}
	}
}

func (c *DeployConfig) ReWrite(confName string) error {
	f, err := os.Create(confName)
	if err != nil {
		log.WarnErrorf(err, "create %s failed.\n", confName)
		return err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.WarnErrorf(err, "Close %s failed.\n", confName)
		}
	}(f)

	w := bufio.NewWriter(f)
	var lineStr string
	for _, item := range c.items {
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
	conf := &DeployConfig{}
	err := conf.Init(defaultConf, sep)
	if err != nil {
		log.WarnErrorf(err, "open  %s file failed.\n", defaultConf)
		return err
	}
	conf.Reset(postConf, isWrap)
	conf.Show()
	var newConf = defaultConf + ".tmp"
	if err = conf.ReWrite(newConf); err != nil {
		return err
	}
	if err = os.Remove(defaultConf); err != nil {
		return err
	}
	return os.Rename(newConf, defaultConf)
}
