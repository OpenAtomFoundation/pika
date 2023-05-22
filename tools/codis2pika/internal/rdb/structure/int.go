package structure

import (
	"encoding/binary"
	"io"
)

func ReadUint8(rd io.Reader) uint8 {
	b := ReadByte(rd)
	return b
}

func ReadUint16(rd io.Reader) uint16 {
	buf := ReadBytes(rd, 2)
	return binary.LittleEndian.Uint16(buf)
}

func ReadUint24(rd io.Reader) uint32 {
	buf := ReadBytes(rd, 3)
	buf = append(buf, 0)
	return binary.LittleEndian.Uint32(buf)
}

func ReadUint32(rd io.Reader) uint32 {
	buf := ReadBytes(rd, 4)
	return binary.LittleEndian.Uint32(buf)
}

func ReadUint64(rd io.Reader) uint64 {
	buf := ReadBytes(rd, 8)
	return binary.LittleEndian.Uint64(buf)
}

func ReadInt8(rd io.Reader) int8 {
	b := ReadByte(rd)
	return int8(b)
}

func ReadInt16(rd io.Reader) int16 {
	buf := ReadBytes(rd, 2)
	return int16(binary.LittleEndian.Uint16(buf))
}

func ReadInt24(rd io.Reader) int32 {
	buf := ReadBytes(rd, 3)
	buf = append([]byte{0}, buf...)
	return int32(binary.LittleEndian.Uint32(buf)) >> 8
}

func ReadInt32(rd io.Reader) int32 {
	buf := ReadBytes(rd, 4)
	return int32(binary.LittleEndian.Uint32(buf))
}

func ReadInt64(rd io.Reader) int64 {
	buf := ReadBytes(rd, 8)
	return int64(binary.LittleEndian.Uint64(buf))
}
