package types

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/rdb/structure"
)

/*
 * The master entry is composed like in the following example:
 *
 *  +-------+---------+------------+---------+--/--+---------+---------+-+
 *	| count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
 *	+-------+---------+------------+---------+--/--+---------+---------+-+

 * Populate the Listpack with the new entry. We use the following
 * encoding:
 *
 * +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
 * |flags|entry-id|num-fields|field-1|value-1|...|field-N|value-N|lp-count|
 * +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
 *
 * However if the SAMEFIELD flag is set, we have just to populate
 * the entry with the values, so it becomes:
 *
 * +-----+--------+-------+-/-+-------+--------+
 * |flags|entry-id|value-1|...|value-N|lp-count|
 * +-----+--------+-------+-/-+-------+--------+
 *
 * The entry-id field is actually two separated fields: the ms
 * and seq difference compared to the master entry.
 *
 * The lp-count field is a number that states the number of Listpack pieces
 * that compose the entry, so that it's possible to travel the entry
 * in reverse order: we can just start from the end of the Listpack, read
 * the entry, and jump back N times to seek the "flags" field to read
 * the stream full entry. */

type StreamObject struct {
	key  string
	cmds []RedisCmd
}

func (o *StreamObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	switch typeByte {
	case rdbTypeStreamListpacks:
		o.readStream(rd, key, typeByte)
	case rdbTypeStreamListpacks2:
		o.readStream(rd, key, typeByte)
	default:
		log.Panicf("unknown hash type. typeByte=[%d]", typeByte)
	}
}

// see redis rewriteStreamObject()

func (o *StreamObject) readStream(rd io.Reader, masterKey string, typeByte byte) {
	// 1. length(number of listpack), k1, v1, k2, v2, ..., number, ms, seq

	/* Load the number of Listpack. */
	nListpack := int(structure.ReadLength(rd))
	for i := 0; i < nListpack; i++ {
		/* Load key */
		key := structure.ReadString(rd)

		/* key is streamId, like: 1612181627287-0 */
		masterMs := int64(binary.BigEndian.Uint64([]byte(key[:8])))
		masterSeq := int64(binary.BigEndian.Uint64([]byte(key[8:])))

		/* value is a listpack */
		elements := structure.ReadListpack(rd)
		inx := 0

		/* The front of stream listpack is master entry */
		/* Parse the master entry */
		count := nextInteger(&inx, elements)          // count
		deleted := nextInteger(&inx, elements)        // deleted
		numFields := int(nextInteger(&inx, elements)) // num-fields

		fields := elements[3 : 3+numFields] // fields
		inx = 3 + numFields

		// master entry end by zero
		lastEntry := nextString(&inx, elements)
		if lastEntry != "0" {
			log.Panicf("master entry not ends by zero. lastEntry=[%s]", lastEntry)
		}

		/* Parse entries */
		for count != 0 || deleted != 0 {
			flags := nextInteger(&inx, elements) // [is_same_fields|is_deleted]
			entryMs := nextInteger(&inx, elements)
			entrySeq := nextInteger(&inx, elements)

			args := []string{"xadd", masterKey, fmt.Sprintf("%v-%v", entryMs+masterMs, entrySeq+masterSeq)}

			if flags&2 == 2 { // same fields, get field from master entry.
				for j := 0; j < numFields; j++ {
					args = append(args, fields[j], nextString(&inx, elements))
				}
			} else { // get field by lp.Next()
				num := int(nextInteger(&inx, elements))
				args = append(args, elements[inx:inx+num*2]...)
				inx += num * 2
			}

			_ = nextString(&inx, elements) // lp_count

			if flags&1 == 1 { // is_deleted
				deleted -= 1
			} else {
				count -= 1
				o.cmds = append(o.cmds, args)
			}
		}
	}

	/* Load total number of items inside the stream. */
	_ = structure.ReadLength(rd) // number

	/* Load the last entry ID. */
	lastMs := structure.ReadLength(rd)
	lastSeq := structure.ReadLength(rd)
	lastid := fmt.Sprintf("%v-%v", lastMs, lastSeq)
	if nListpack == 0 {
		/* Use the XADD MAXLEN 0 trick to generate an empty stream if
		 * the key we are serializing is an empty string, which is possible
		 * for the Stream type. */
		args := []string{"xadd", masterKey, "MAXLEN", "0", lastid, "x", "y"}
		o.cmds = append(o.cmds, args)
	}

	/* Append XSETID after XADD, make sure lastid is correct,
	 * in case of XDEL lastid. */
	o.cmds = append(o.cmds, []string{"xsetid", masterKey, lastid})

	if typeByte == rdbTypeStreamListpacks2 {
		/* Load the first entry ID. */
		_ = structure.ReadLength(rd) // first_ms
		_ = structure.ReadLength(rd) // first_seq

		/* Load the maximal deleted entry ID. */
		_ = structure.ReadLength(rd) // max_deleted_ms
		_ = structure.ReadLength(rd) // max_deleted_seq

		/* Load the offset. */
		_ = structure.ReadLength(rd) // offset
	}

	/* 2. nConsumerGroup, groupName, ms, seq, PEL, Consumers */

	/* Load the number of groups. */
	nConsumerGroup := int(structure.ReadLength(rd))
	for i := 0; i < nConsumerGroup; i++ {
		/* Load groupName */
		groupName := structure.ReadString(rd)

		/* Load the last ID */
		lastMs := structure.ReadLength(rd)
		lastSeq := structure.ReadLength(rd)
		lastid := fmt.Sprintf("%v-%v", lastMs, lastSeq)

		/* Create Group */
		o.cmds = append(o.cmds, []string{"CREATE", masterKey, groupName, lastid})

		/* Load group offset. */
		if typeByte == rdbTypeStreamListpacks2 {
			_ = structure.ReadLength(rd) // offset
		}

		/* Load the global PEL */
		nPel := int(structure.ReadLength(rd))
		mapId2Time := make(map[string]uint64)
		mapId2Count := make(map[string]uint64)

		for j := 0; j < nPel; j++ {
			/* Load streamId */
			tmpBytes := structure.ReadBytes(rd, 16)
			ms := binary.BigEndian.Uint64(tmpBytes[:8])
			seq := binary.BigEndian.Uint64(tmpBytes[8:])
			streamId := fmt.Sprintf("%v-%v", ms, seq)

			/* Load deliveryTime */
			deliveryTime := structure.ReadUint64(rd)

			/* Load deliveryCount */
			deliveryCount := structure.ReadLength(rd)

			/* Save deliveryTime and deliveryCount  */
			mapId2Time[streamId] = deliveryTime
			mapId2Count[streamId] = deliveryCount
		}

		/* Generate XCLAIMs for each consumer that happens to
		 * have pending entries. Empty consumers are discarded. */
		nConsumer := int(structure.ReadLength(rd))
		for j := 0; j < nConsumer; j++ {
			/* Load consumerName */
			consumerName := structure.ReadString(rd)

			/* Load lastSeenTime */
			_ = structure.ReadUint64(rd)

			/* Consumer PEL */
			nPEL := int(structure.ReadLength(rd))
			for i := 0; i < nPEL; i++ {

				/* Load streamId */
				tmpBytes := structure.ReadBytes(rd, 16)
				ms := binary.BigEndian.Uint64(tmpBytes[:8])
				seq := binary.BigEndian.Uint64(tmpBytes[8:])
				streamId := fmt.Sprintf("%v-%v", ms, seq)

				/* Send */
				args := []string{
					"xclaim", masterKey, groupName, consumerName, "0", streamId,
					"TIME", strconv.FormatUint(mapId2Time[streamId], 10),
					"RETRYCOUNT", strconv.FormatUint(mapId2Count[streamId], 10),
					"JUSTID", "FORCE"}
				o.cmds = append(o.cmds, args)
			}
		}
	}
}

func nextInteger(inx *int, elements []string) int64 {
	ele := elements[*inx]
	*inx++
	i, err := strconv.ParseInt(ele, 10, 64)
	if err != nil {
		log.Panicf("integer is not a number. ele=[%s]", ele)
	}
	return i
}

func nextString(inx *int, elements []string) string {
	ele := elements[*inx]
	*inx++
	return ele
}

func (o *StreamObject) Rewrite() []RedisCmd {
	return o.cmds
}
