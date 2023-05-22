package structure

import (
	"io"
	"strconv"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
)

const (
	RDBEncInt8  = 0 // RDB_ENC_INT8
	RDBEncInt16 = 1 // RDB_ENC_INT16
	RDBEncInt32 = 2 // RDB_ENC_INT32
	RDBEncLZF   = 3 // RDB_ENC_LZF
)

func ReadString(rd io.Reader) string {
	length, special, err := readEncodedLength(rd)

	if err != nil {
		log.PanicError(err)
	}
	if special {
		switch length {
		case RDBEncInt8:
			b := ReadInt8(rd)
			return strconv.Itoa(int(b))
		case RDBEncInt16:
			b := ReadInt16(rd)
			return strconv.Itoa(int(b))
		case RDBEncInt32:
			b := ReadInt32(rd)
			return strconv.Itoa(int(b))
		default:
			log.Panicf("Unknown string encode type %d", length)
		}
	}
	return string(ReadBytesAll(rd, int(length)))
}

func lzfDecompress(in []byte, outLen int) string {
	out := make([]byte, outLen)

	i, o := 0, 0
	for i < len(in) {
		ctrl := int(in[i])
		i++
		if ctrl < 32 {
			for x := 0; x <= ctrl; x++ {
				out[o] = in[i]
				i++
				o++
			}
		} else {
			length := ctrl >> 5
			if length == 7 {
				length = length + int(in[i])
				i++
			}
			ref := o - ((ctrl & 0x1f) << 8) - int(in[i]) - 1
			i++
			for x := 0; x <= length+1; x++ {
				out[o] = out[ref]
				ref++
				o++
			}
		}
	}
	if o != outLen {
		log.Panicf("lzf decompress failed: outLen: %d, o: %d", outLen, o)
	}
	return string(out)
}
