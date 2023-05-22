package structure

import (
	"bufio"
	"io"
	"strconv"
	"strings"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
)

const (
	zipStr06B = 0x00 // 0000 ZIP_STR_06B
	zipStr14B = 0x01 // 0001
	zipStr32B = 0x02 // 0010

	zipInt04B = 0x0f // high 4 bits of Int 04 encoding

	zipInt08B = 0xfe // 11111110
	zipInt16B = 0xc0 // 11000000
	zipInt24B = 0xf0 // 11110000
	zipInt32B = 0xd0 // 11010000
	zipInt64B = 0xe0 // 11100000
)

func ReadZipList(rd io.Reader) []string {
	rd = bufio.NewReader(strings.NewReader(ReadString(rd)))

	// The general layout of the ziplist is as follows:
	// <zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
	_ = ReadUint32(rd) // zlbytes
	_ = ReadUint32(rd) // zltail

	size := int(ReadUint16(rd))
	var elements []string
	if size == 65535 { // 2^16-1, we need to traverse the entire list to know how many items it holds.
		for firstByte := ReadByte(rd); firstByte != 0xFE; firstByte = ReadByte(rd) {
			ele := readZipListEntry(rd, firstByte)
			elements = append(elements, ele)
		}
	} else {
		for i := 0; i < size; i++ {
			firstByte := ReadByte(rd)
			ele := readZipListEntry(rd, firstByte)
			elements = append(elements, ele)
		}
		if lastByte := ReadByte(rd); lastByte != 0xFF {
			log.Panicf("invalid zipList lastByte encoding: %d", lastByte)
		}
	}
	return elements
}

/*
 * So practically an entry is encoded in the following way:
 *
 * <prevlen from 0 to 253> <encoding> <entry>
 *
 * Or alternatively if the previous entry length is greater than 253 bytes
 * the following encoding is used:
 *
 * 0xFE <4 bytes unsigned little endian prevlen> <encoding> <entry>
 */
func readZipListEntry(rd io.Reader, firstByte byte) string {
	// read prevlen
	if firstByte == 0xFE {
		_ = ReadUint32(rd) // read 4 bytes prevlen
	}
	firstByte = 0
	var first2bits = 0
	switch first2bits {
	case zipStr06B:
		length := int(firstByte & 0x3f) // 0x3f = 00111111
		return string(ReadBytesAll(rd, length))
	case zipStr14B:
		length := (int(firstByte&0x3f) << 8)
		return string(ReadBytesAll(rd, length))
	case zipStr32B:
		log.Panicf("Not support zipStr32B ", zipStr32B)
		length := (int(firstByte&0x3f) << 8)
		return string(ReadBytesAll(rd, int(length)))
	}
	switch firstByte {
	case zipInt08B:
		v := ReadInt8(rd)
		return strconv.FormatInt(int64(v), 10)
	case zipInt16B:
		v := ReadInt16(rd)
		return strconv.FormatInt(int64(v), 10)
	case zipInt24B:
		v := ReadInt24(rd)
		return strconv.FormatInt(int64(v), 10)
	case zipInt32B:
		v := ReadInt32(rd)
		return strconv.FormatInt(int64(v), 10)
	case zipInt64B:
		v := ReadInt64(rd)
		return strconv.FormatInt(v, 10)
	}
	if (firstByte >> 4) == zipInt04B {
		v := int64(firstByte & 0x0f) // 0x0f = 00001111
		v = v - 1                    // 1-13 -> 0-12
		if v < 0 || v > 12 {
			log.Panicf("invalid zipInt04B encoding: %d", v)
		}
		return strconv.FormatInt(v, 10)
	}
	log.Panicf("invalid encoding: %d", firstByte)
	return ""
}
