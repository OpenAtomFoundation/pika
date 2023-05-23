package writer

import (
	"bytes"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/client"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/client/proto"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/config"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/entry"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/log"
	"github.com/OpenAtomFoundation/pika/tools/codis2pika/internal/statistics"
)

type authInfo struct {
	address  string
	username string
	password string
	isTls    bool
}

// var authInfoChan chan *authInfo
var authInfoSingle *authInfo
var slotauths map[int]*authInfo
var addressauths map[string]*authInfo

func init() {
	//设置队列长度等于最大重试次数
	slotauths = make(map[int]*authInfo, 1024)
	addressauths = make(map[string]*authInfo, 100)
}

type redisWriter struct {
	client *client.Redis
	DbId   int

	cmdBuffer   *bytes.Buffer
	chWaitReply chan *entry.Entry

	UpdateUnansweredBytesCount uint64 // have sent in bytes
}

func newWriteClientPool(address string, username string, password string, isTls bool) {
	getauthinfo := authInfo{address, username, password, isTls}
	if config.Config.Target.Type != "cluster" {
		authInfoSingle = &getauthinfo
	} else {
		addressauths[address] = &getauthinfo
	}
}

func getWriteConn() *client.Redis {
	authinfo := authInfoSingle
	newclient := client.NewRedisClient(authinfo.address, authinfo.username, authinfo.password, authinfo.isTls)
	return newclient
}

func getClusterWriteConn(slotauths map[int]*authInfo, slots []int) *client.Redis {
	//每条消息只有一个slot
	v := slots[0]
	authinfo := slotauths[v]
	newclient := client.NewRedisClient(authinfo.address, authinfo.username, authinfo.password, authinfo.isTls)
	return newclient
}

func NewRedisWriter(address string, username string, password string, isTls bool) Writer {
	//初始化redisWriter结构体
	rw := new(redisWriter)

	if config.Config.Target.Type != "cluster" {
		//初始化用户信息
		newWriteClientPool(address, username, password, isTls)
		//新建redis client
		rw.client = getWriteConn()
	} else {
		//集群模式直接创建连接客户端
		rw.client = client.NewRedisClient(address, username, password, isTls)
	}
	log.Infof("redisWriter connected to redis successful. address=[%s]", address)
	rw.cmdBuffer = new(bytes.Buffer)
	//根据配置文件中的队列长度创建channel
	rw.chWaitReply = make(chan *entry.Entry, config.Config.Advanced.PipelineCountLimit)
	//协程刷新数据，更新迁移情况
	go rw.flushInterval()
	return rw
}

func (w *redisWriter) Write(e *entry.Entry) {
	// recover when net.conn broken pipe
	defer func() {
		if err := recover(); err != nil {
			log.Warnf("This is broken pipe error: %v", err)
			log.Infof("try again %v ", e.Argv)
			if config.Config.Target.Type != "cluster" {
				//获取新的redis client
				w.client = getWriteConn()
				//获取返回信息
				w.chWaitReply <- e
				//再次写入
				w.client.SendBytes(w.cmdBuffer.Bytes())
			} else {
				w.client = getClusterWriteConn(slotauths, e.Slots)
				//获取返回信息
				w.chWaitReply <- e
				//再次写入
				w.client.SendBytes(w.cmdBuffer.Bytes())
			}
			log.Infof("recover finish. %v ", e.Argv)
		}
	}()
	// switch db if we need
	if w.DbId != e.DbId {
		w.switchDbTo(e.DbId)
	}

	// send
	w.cmdBuffer.Reset()

	//获取接口参数，并将buffer的cmd写入
	client.EncodeArgv(e.Argv, w.cmdBuffer)
	//计算entry的key值大小是否超过大key的阈值限制
	e.EncodedSize = uint64(w.cmdBuffer.Len())
	for e.EncodedSize+atomic.LoadUint64(&w.UpdateUnansweredBytesCount) > config.Config.Advanced.TargetRedisClientMaxQuerybufLen {
		time.Sleep(1 * time.Nanosecond)
	}
	w.chWaitReply <- e
	atomic.AddUint64(&w.UpdateUnansweredBytesCount, e.EncodedSize)
	//将buf中的byte写入 write并flush
	w.client.SendBytes(w.cmdBuffer.Bytes())
}

func (w *redisWriter) switchDbTo(newDbId int) {
	w.client.Send("select", strconv.Itoa(newDbId))
	w.DbId = newDbId
}

func (w *redisWriter) flushInterval() {
	for {
		select {
		//从chan *entry.Entry通道中获取消息
		case e := <-w.chWaitReply:
			reply, err := w.client.Receive()
			//定义的常量值
			if err == proto.Nil {
				log.Warnf("redisWriter receive nil reply. argv=%v", e.Argv)
			} else if err != nil {
				if err.Error() == "BUSYKEY Target key name already exists." {
					if config.Config.Advanced.RDBRestoreCommandBehavior == "skip" {
						log.Warnf("redisWriter received BUSYKEY reply. argv=%v", e.Argv)
					} else if config.Config.Advanced.RDBRestoreCommandBehavior == "panic" {
						log.Panicf("redisWriter received BUSYKEY reply. argv=%v", e.Argv)
					}
					// 当写入发生panic时，此时的client已经关闭，无法再读读取，返回EOF
				} else if err.Error() == "EOF" {
					log.Warnf("redisWriter received EOF. error=[%v], argv=%v, slots=%v, reply=[%v]", err, e.Argv, e.Slots, reply)
					log.Infof("try again %v ", e.Argv)
					if config.Config.Target.Type == "cluster" {
						w.client = getClusterWriteConn(slotauths, e.Slots)
					} else {
						w.client = getWriteConn()
					}
					w.Write(e)
					log.Infof("finish ")
				} else {
					log.Panicf("redisWriter received error. error=[%v], argv=%v, slots=%v, reply=[%v]", err, e.Argv, e.Slots, reply)
				}
			}
			atomic.AddUint64(&w.UpdateUnansweredBytesCount, ^(e.EncodedSize - 1))
			statistics.UpdateEntryId(e.Id)
			statistics.UpdateAOFAppliedOffset(e.Offset)
			statistics.UpdateUnansweredBytesCount(atomic.LoadUint64(&w.UpdateUnansweredBytesCount))
		}
	}
}
