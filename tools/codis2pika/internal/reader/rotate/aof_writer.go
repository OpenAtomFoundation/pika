package rotate

import (
	"fmt"
	"os"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
)

const MaxFileSize = 1024 * 1024 * 1024 // 1G

type AOFWriter struct {
	file     *os.File
	offset   int64
	filename string
	filesize int64
}

func NewAOFWriter(offset int64) *AOFWriter {
	w := &AOFWriter{}
	w.openFile(offset)
	return w
}

// 初始化时，将传入的offset设为偏移量
func (w *AOFWriter) openFile(offset int64) {
	w.filename = fmt.Sprintf("%d.aof", offset)
	var err error
	w.file, err = os.OpenFile(w.filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.PanicError(err)
	}
	w.offset = offset
	w.filesize = 0
	log.Infof("AOFWriter open file. filename=[%s]", w.filename)
}

// 每次写入buf，都会更新累加AOFWriter.offset
func (w *AOFWriter) Write(buf []byte) {
	_, err := w.file.Write(buf)
	if err != nil {
		log.PanicError(err)
	}
	w.offset += int64(len(buf))
	w.filesize += int64(len(buf))
	if w.filesize > MaxFileSize {
		w.Close()
		w.openFile(w.offset)
	}
	err = w.file.Sync()
	if err != nil {
		log.PanicError(err)
	}
}

func (w *AOFWriter) Close() {
	if w.file == nil {
		return
	}
	err := w.file.Sync()
	if err != nil {
		log.PanicError(err)
	}
	err = w.file.Close()
	if err != nil {
		log.PanicError(err)
	}
	log.Infof("AOFWriter close file. filename=[%s], filesize=[%d]", w.filename, w.filesize)
}
