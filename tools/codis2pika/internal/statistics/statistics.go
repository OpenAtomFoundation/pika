package statistics

import (
	"time"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/config"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
)

var (
	// ID
	entryId uint64
	// rdb
	rdbFileSize     int64
	rdbReceivedSize int64
	rdbSendSize     int64
	// aof
	aofReceivedOffset int64
	aofAppliedOffset  int64
	// ops
	allowEntriesCount    int64
	disallowEntriesCount int64
	unansweredBytesCount uint64
)

func Init() {
	go func() {
		//配置文件中的log_interval 默认为5s
		seconds := config.Config.Advanced.LogInterval
		if seconds <= 0 {
			log.Infof("statistics disabled. seconds=[%d]", seconds)
		}

		for range time.Tick(time.Duration(seconds) * time.Second) {
			if rdbFileSize == 0 {
				continue
			}
			if rdbFileSize > rdbReceivedSize {
				log.Infof("receiving rdb. percent=[%.2f]%%, rdbFileSize=[%.3f]G, rdbReceivedSize=[%.3f]G",
					float64(rdbReceivedSize)/float64(rdbFileSize)*100,
					float64(rdbFileSize)/1024/1024/1024,
					float64(rdbReceivedSize)/1024/1024/1024)
			} else if rdbFileSize > rdbSendSize {
				log.Infof("syncing rdb.  entryId=[%d], unansweredBytesCount=[%d]bytes, rdbFileSize=[%.3f]G",
					entryId,
					unansweredBytesCount,
					float64(rdbFileSize)/1024/1024/1024)
				// 当 rdbFileSize == rdbSendSize 时，开始同步aof文件
			} else {
				log.Infof("syncing aof.  entryId=[%d], unansweredBytesCount=[%d]bytes, diff=[%d]",
					entryId,
					unansweredBytesCount,
					aofReceivedOffset-aofAppliedOffset)
			}

			allowEntriesCount = 0
			disallowEntriesCount = 0
		}
	}()
}
func UpdateEntryId(id uint64) {
	entryId = id
}
func AddAllowEntriesCount() {
	allowEntriesCount++
}
func AddDisallowEntriesCount() {
	disallowEntriesCount++
}

// 将输出的size大小，设置为rdb文件的大小
func SetRDBFileSize(size int64) {
	rdbFileSize = size
}
func UpdateRDBReceivedSize(size int64) {
	rdbReceivedSize = size
}
func UpdateRDBSentSize(offset int64) {
	rdbSendSize = offset
}

// 将入参设置为aof
func UpdateAOFReceivedOffset(offset int64) {
	aofReceivedOffset = offset
}
func UpdateAOFAppliedOffset(offset int64) {
	aofAppliedOffset = offset
}
func UpdateUnansweredBytesCount(count uint64) {
	unansweredBytesCount = count
}
