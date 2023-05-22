package structure

import (
	"bufio"
	"encoding/binary"
	"io"
	"strconv"
	"strings"
)

func ReadIntset(rd io.Reader) []string {
	rd = bufio.NewReader(strings.NewReader(ReadString(rd)))

	encodingType := int(ReadUint32(rd))
	size := int(ReadUint32(rd))
	elements := make([]string, size)

	for i := 0; i < size; i++ {
		intBytes := ReadBytes(rd, encodingType)
		var intString string
		switch encodingType {
		case 2:
			intString = strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)
		case 4:
			intString = strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)
		case 8:
			intString = strconv.FormatInt(int64(int64(binary.LittleEndian.Uint64(intBytes))), 10)
		}
		elements[i] = intString
	}
	return elements
}
