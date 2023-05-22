package reader

import (
	"path/filepath"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/entry"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/rdb"
)

type rdbReader struct {
	path string
	ch   chan *entry.Entry
}

func NewRDBReader(path string) Reader {
	log.Infof("NewRDBReader: path=[%s]", path)
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		log.Panicf("NewRDBReader: filepath.Abs error: %s", err.Error())
	}
	log.Infof("NewRDBReader: absolute path=[%s]", absolutePath)
	r := new(rdbReader)
	r.path = absolutePath
	return r
}

func (r *rdbReader) StartRead() chan *entry.Entry {
	r.ch = make(chan *entry.Entry, 1024)

	go func() {
		// start parse rdb
		log.Infof("start send RDB. path=[%s]", r.path)
		rdbLoader := rdb.NewLoader(r.path, r.ch)
		//rdb模块解析了DB，并直接发送到了通道中
		_ = rdbLoader.NewParseRDB()
		log.Infof("send RDB finished. path=[%s]", r.path)
		close(r.ch)
	}()

	return r.ch
}
