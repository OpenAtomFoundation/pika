package filter

import (
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/entry"

	lua "github.com/yuin/gopher-lua"
)

const (
	Allow    = 0
	Disallow = 1
	Error    = 2
)

var luaInstance *lua.LState

func LoadFromFile(luaFile string) {
	luaInstance = lua.NewState()
	err := luaInstance.DoFile(luaFile)
	if err != nil {
		panic(err)
	}
}

func Filter(e *entry.Entry) int {
	if luaInstance == nil {
		return Allow
	}
	keys := luaInstance.NewTable()
	for _, key := range e.Keys {
		keys.Append(lua.LString(key))
	}

	slots := luaInstance.NewTable()
	for _, slot := range e.Slots {
		slots.Append(lua.LNumber(slot))
	}

	f := luaInstance.GetGlobal("filter")
	luaInstance.Push(f)
	luaInstance.Push(lua.LNumber(e.Id))          // id
	luaInstance.Push(lua.LBool(e.IsBase))        // is_base
	luaInstance.Push(lua.LString(e.Group))       // group
	luaInstance.Push(lua.LString(e.CmdName))     // cmd name
	luaInstance.Push(keys)                       // keys
	luaInstance.Push(slots)                      // slots
	luaInstance.Push(lua.LNumber(e.DbId))        // dbid
	luaInstance.Push(lua.LNumber(e.TimestampMs)) // timestamp_ms

	luaInstance.Call(8, 2)

	code := int(luaInstance.Get(1).(lua.LNumber))
	e.DbId = int(luaInstance.Get(2).(lua.LNumber))
	luaInstance.Pop(2)
	return code
}
