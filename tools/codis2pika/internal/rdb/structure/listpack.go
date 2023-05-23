package structure

import (
	"bufio"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
)

const (
	lpEncoding7BitUintMask = 0x80 // 10000000 LP_ENCODING_7BIT_UINT_MASK
	lpEncoding7BitUint     = 0x00 // 00000000 LP_ENCODING_7BIT_UINT

	lpEncoding6BitStrMask = 0xC0 // 11000000 LP_ENCODING_6BIT_STR_MASK
	lpEncoding6BitStr     = 0x80 // 10000000 LP_ENCODING_6BIT_STR

	lpEncoding13BitIntMask = 0xE0 // 11100000 LP_ENCODING_13BIT_INT_MASK
	lpEncoding13BitInt     = 0xC0 // 11000000 LP_ENCODING_13BIT_INT

	lpEncoding12BitStrMask = 0xF0 // 11110000 LP_ENCODING_12BIT_STR_MASK
	lpEncoding12BitStr     = 0xE0 // 11100000 LP_ENCODING_12BIT_STR

	lpEncoding16BitIntMask = 0xFF // 11111111 LP_ENCODING_16BIT_INT_MASK
	lpEncoding16BitInt     = 0xF1 // 11110001 LP_ENCODING_16BIT_INT

	lpEncoding24BitIntMask = 0xFF // 11111111 LP_ENCODING_24BIT_INT_MASK
	lpEncoding24BitInt     = 0xF2 // 11110010 LP_ENCODING_24BIT_INT

	lpEncoding32BitIntMask = 0xFF // 11111111 LP_ENCODING_32BIT_INT_MASK
	lpEncoding32BitInt     = 0xF3 // 11110011 LP_ENCODING_32BIT_INT

	lpEncoding64BitIntMask = 0xFF // 11111111 LP_ENCODING_64BIT_INT_MASK
	lpEncoding64BitInt     = 0xF4 // 11110100 LP_ENCODING_64BIT_INT

	lpEncoding32BitStrMask = 0xFF // 11111111 LP_ENCODING_32BIT_STR_MASK
	lpEncoding32BitStr     = 0xF0 // 11110000 LP_ENCODING_32BIT_STR
)

func ReadListpack(rd io.Reader) []string {
	rd = bufio.NewReader(strings.NewReader(ReadString(rd)))

	_ = ReadUint32(rd) // bytes
	size := int(ReadUint16(rd))
	var elements []string
	for i := 0; i < size; i++ {
		ele := readListpackEntry(rd)
		elements = append(elements, ele)
	}
	lastByte := ReadByte(rd)
	if lastByte != 0xFF {
		log.Panicf("ReadListpack: last byte is not 0xFF, but [%d]", lastByte)
	}
	return elements
}

// redis/src/Listpack.c lpGet()
func readListpackEntry(rd io.Reader) string {
	var val int64
	var uval, negstart, negmax uint64
	fireByte := ReadByte(rd)
	if (fireByte & lpEncoding7BitUintMask) == lpEncoding7BitUint { // 7bit uint

		uval = uint64(fireByte & 0x7f) // 0x7f is 01111111
		negmax = 0
		negstart = math.MaxUint64             // uint
		_ = ReadBytes(rd, lpEncodeBacklen(1)) // encode: 1 byte

	} else if (fireByte & lpEncoding6BitStrMask) == lpEncoding6BitStr { // 6bit length str

		length := int(fireByte & 0x3f) // 0x3f is 00111111
		ele := string(ReadBytes(rd, length))
		_ = ReadBytes(rd, lpEncodeBacklen(1+length)) // encode: 1byte, str: length
		return ele

	} else if (fireByte & lpEncoding13BitIntMask) == lpEncoding13BitInt { // 13bit int

		secondByte := ReadByte(rd)
		uval = (uint64(fireByte&0x1f) << 8) + uint64(secondByte) // 5bit + 8bit, 0x1f is 00011111
		negstart = uint64(1) << 12
		negmax = 8191 // uint13_max
		_ = ReadBytes(rd, lpEncodeBacklen(2))

	} else if (fireByte & lpEncoding16BitIntMask) == lpEncoding16BitInt { // 16bit int

		uval = uint64(ReadUint16(rd))
		negstart = uint64(1) << 15
		negmax = 65535                        // uint16_max
		_ = ReadBytes(rd, lpEncodeBacklen(2)) // encode: 1byte, int: 2byte

	} else if (fireByte & lpEncoding24BitIntMask) == lpEncoding24BitInt { // 24bit int

		uval = uint64(ReadUint24(rd))
		negstart = uint64(1) << 23
		negmax = math.MaxUint32 >> 8            // uint24_max
		_ = ReadBytes(rd, lpEncodeBacklen(1+3)) // encode: 1byte, int: 3byte

	} else if (fireByte & lpEncoding32BitIntMask) == lpEncoding32BitInt { // 32bit int

		uval = uint64(ReadUint32(rd))
		negstart = uint64(1) << 31
		negmax = math.MaxUint32                 // uint32_max
		_ = ReadBytes(rd, lpEncodeBacklen(1+4)) // encode: 1byte, int: 4byte

	} else if (fireByte & lpEncoding64BitIntMask) == lpEncoding64BitInt { // 64bit int

		uval = ReadUint64(rd)
		negstart = uint64(1) << 63
		negmax = math.MaxUint64                 // uint64_max
		_ = ReadBytes(rd, lpEncodeBacklen(1+8)) // encode: 1byte, int: 8byte

	} else if (fireByte & lpEncoding12BitStrMask) == lpEncoding12BitStr { // 12bit length str

		secondByte := ReadByte(rd)
		length := (int(fireByte&0x0f) << 8) + int(secondByte) // 4bit + 8bit
		ele := string(ReadBytes(rd, length))
		_ = ReadBytes(rd, lpEncodeBacklen(2+length)) // encode: 2byte, str: length
		return ele

	} else if (fireByte & lpEncoding32BitStrMask) == lpEncoding32BitStr { // 32bit length str

		length := int(ReadUint32(rd))
		ele := string(ReadBytes(rd, length))
		_ = ReadBytes(rd, lpEncodeBacklen(5+length)) // encode: 1byte, length: 4byte, str: length
		return ele

	} else {
		// redis use this value, don't know why
		// uval = 12345678900000000 + uint64(fireByte)
		// negstart = math.MaxUint64
		// negmax = 0
		log.Panicf("unknown encoding: %x", fireByte)
	}

	/* We reach this code path only for integer encodings.
	 * Convert the unsigned value to the signed one using two's complement
	 * rule. */
	if uval >= negstart {
		/* This three steps conversion should avoid undefined behaviors
		 * in the unsigned -> signed conversion. */

		uval = negmax - uval
		val = int64(uval)
		val = -val - 1
	} else {
		val = int64(uval)
	}

	return strconv.FormatInt(val, 10)
}

/* the function just returns the length(byte) of `backlen`. */
func lpEncodeBacklen(len int) int {
	if len <= 127 {
		return 1
	} else if len < 16383 {
		return 2
	} else if len < 2097151 {
		return 3
	} else if len < 268435455 {
		return 4
	} else {
		return 5
	}
}
