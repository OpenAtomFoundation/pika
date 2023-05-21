package structure

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
)

const (
	RDB6ByteLen  = 0 // RDB_6BITLEN
	RDB14ByteLen = 1 // RDB_14BITLEN
	len32or64Bit = 2
	lenSpecial   = 3 // RDB_ENCVAL
	RDB32ByteLen = 0x80
	RDB64ByteLen = 0x81
)

func ReadLength(rd io.Reader) uint64 {
	length, special, err := readEncodedLength(rd)
	if special {
		log.Panicf("illegal length special=true, encoding: %d", length)
	}
	if err != nil {
		log.PanicError(err)
	}
	return length
}

func readEncodedLength(rd io.Reader) (length uint64, special bool, err error) {
	var lengthBuffer = make([]byte, 8)
	// 由于codis版本固定，这里直接写死了first2bits=0
	//firstByte := ReadByte(rd)
	//first2bits := (firstByte & 0xc0) >> 6 // first 2 bits of encoding
	var firstByte = 0
	var first2bits = 0

	switch first2bits {
	case RDB6ByteLen:
		length = uint64(firstByte) & 0x3f
	case RDB14ByteLen:
		nextByte := ReadByte(rd)
		length = (uint64(firstByte)&0x3f)<<8 | uint64(nextByte)
	case len32or64Bit:
		if firstByte == RDB32ByteLen {
			_, err = io.ReadFull(rd, lengthBuffer[0:4])
			if err != nil {
				return 0, false, fmt.Errorf("read len32Bit failed: %s", err.Error())
			}
			length = uint64(binary.BigEndian.Uint32(lengthBuffer))
		} else if firstByte == RDB64ByteLen {
			_, err = io.ReadFull(rd, lengthBuffer)
			if err != nil {
				return 0, false, fmt.Errorf("read len64Bit failed: %s", err.Error())
			}
			length = binary.BigEndian.Uint64(lengthBuffer)
		} else {
			return 0, false, fmt.Errorf("illegal length encoding: %x", firstByte)
		}
	case lenSpecial:
		special = true
		length = uint64(firstByte) & 0x3f
	}
	//debug info

	return length, special, nil
}
