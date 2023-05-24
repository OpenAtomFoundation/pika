package utils

import "testing"

func TestCrc16(t *testing.T) {
	ret := Crc16("123456789")
	if ret != 0x31c3 {
		t.Errorf("Crc16(123456789) = %x", ret)
	}
}
