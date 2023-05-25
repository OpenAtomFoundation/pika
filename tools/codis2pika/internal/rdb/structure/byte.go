package structure

import (
	"io"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
)

func ReadByte(rd io.Reader) byte {
	b := ReadBytes(rd, 1)[0]
	return b
}

func ReadBytes(rd io.Reader, n int) []byte {
	buf := make([]byte, n)
	_, err := io.ReadFull(rd, buf)

	if err != nil {
		log.PanicError(err)
	}
	return buf
}
func ReadBytesAll(rd io.Reader, n int) []byte {

	allread, err := io.ReadAll(rd)

	if err != nil {
		log.PanicError(err)
	}
	return allread
}
