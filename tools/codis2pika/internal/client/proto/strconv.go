package proto

import "strconv"

func bytesToString(b []byte) string {
	return string(b)
}

func stringToBytes(s string) []byte {
	return []byte(s)
}

func atoi(b []byte) (int, error) {
	return strconv.Atoi(bytesToString(b))
}

func parseInt(b []byte, base int, bitSize int) (int64, error) {
	return strconv.ParseInt(bytesToString(b), base, bitSize)
}

func parseUint(b []byte, base int, bitSize int) (uint64, error) {
	return strconv.ParseUint(bytesToString(b), base, bitSize)
}

func parseFloat(b []byte, bitSize int) (float64, error) {
	return strconv.ParseFloat(bytesToString(b), bitSize)
}
