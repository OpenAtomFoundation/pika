package commands

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
)

// CalcKeys https://redis.io/docs/reference/key-specs/
func CalcKeys(argv []string) (cmaName string, group string, keys []string) {
	argc := len(argv)
	group = "unknown"

	cmaName = strings.ToUpper(argv[0])
	if _, ok := containers[cmaName]; ok {
		cmaName = fmt.Sprintf("%s-%s", cmaName, strings.ToUpper(argv[1]))
	}
	cmd, ok := redisCommands[cmaName]
	if !ok {
		log.Warnf("unknown command. argv=%v", argv)
		return
	}
	group = cmd.group
	for _, spec := range cmd.keySpec {
		begin := 0
		switch spec.beginSearchType {
		case "index":
			begin = spec.beginSearchIndex
		case "keyword":
			var inx, step int
			if spec.beginSearchStartFrom > 0 {
				inx = spec.beginSearchStartFrom
				step = 1
			} else {
				inx = -spec.beginSearchStartFrom
				step = -1
			}
			for ; ; inx += step {
				if inx == argc {
					log.Panicf("not found keyword. argv=%v", argv)
				}
				if strings.ToUpper(argv[inx]) == spec.beginSearchKeyword {
					begin = inx + 1
					break
				}
			}
		default:
			log.Panicf("wrong type: %s", spec.beginSearchType)
		}
		switch spec.findKeysType {
		case "range":
			var lastKeyInx int
			if spec.findKeysRangeLastKey >= 0 {
				lastKeyInx = begin + spec.findKeysRangeLastKey
			} else {
				lastKeyInx = argc + spec.findKeysRangeLastKey
			}
			limitCount := math.MaxInt32
			if spec.findKeysRangeLimit <= -2 {
				limitCount = (argc - begin) / (-spec.findKeysRangeLimit)
			}
			keyStep := spec.findKeysRangeKeyStep
			for inx := begin; inx <= lastKeyInx && limitCount > 0; inx += keyStep {
				keys = append(keys, argv[inx])
				limitCount -= 1
			}
		case "keynum":
			keynumIdx := begin + spec.findKeysKeynumIndex
			if keynumIdx < 0 || keynumIdx > argc {
				log.Panicf("keynumInx wrong. argv=%v, keynumIdx=[%d]", argv, keynumIdx)
			}
			keyCount, err := strconv.Atoi(argv[keynumIdx])
			if err != nil {
				log.PanicError(err)
			}
			firstKey := spec.findKeysKeynumFirstKey
			step := spec.findKeysKeynumKeyStep
			for inx := begin + firstKey; keyCount > 0; inx += step {
				keys = append(keys, argv[inx])
				keyCount -= 1
			}
		default:
			log.Panicf("wrong type: %s", spec.findKeysType)
		}
	}
	return
}
