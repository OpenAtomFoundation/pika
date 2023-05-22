package entry

import "fmt"

type Entry struct {
	Id          uint64
	IsBase      bool //  whether the command is decoded from dump.rdb file
	DbId        int
	Argv        []string
	TimestampMs uint64

	CmdName string
	Group   string
	Keys    []string
	Slots   []int

	// for statistics
	Offset      int64
	EncodedSize uint64 // the size of the entry after encode
}

func NewEntry() *Entry {
	e := new(Entry)
	return e
}

func (e *Entry) ToString() string {
	return fmt.Sprintf("%v", e.Argv)
}
