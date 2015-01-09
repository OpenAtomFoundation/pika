#ifndef __CONSUMER_H__
#define __CONSUMER_H__

#include "SimpleConsumer.h"
#include "SimpleVector.h"
#include "SimpleHashtab.h"
#include <string>
#include <time.h>
#include <sys/types.h>

//namespace Qbus {

typedef struct {
    int partition_;
    char brokerId_[16];
    char brokerIp_[16];
    int brokerPort_;
    int64_t offset_;
    KafkaSimpleConsumer *consumer_;
}PartitionInfo_t;

typedef struct {
    time_t prePollTime_;
    std::string topic_;
    std::string groupId_;
    std::string preConsumePart_;
    SimpleVector *partInfo_;
}ProcessInfo_t;

class KafkaConsumer {
public:
    ProcessInfo_t topicProcessInfo_;

private:
    std::string consumerId_;
    std::string zkServers_;
    void *zhandle_;
    int64_t processNum_;
    Hashtab *consumerMap_;
public:
    KafkaConsumer(std::string &cluster, std::string &topic, std::string &groupId);
    ~KafkaConsumer();
    const std::string& getConsumerId() const;
    int reconnectZookeeper();
    bool reloadPartionInfo(bool *needExit);
    KafkaMessageSet* fetchPartitionMsg(std::string topic, std::string groupId, PartitionInfo_t *partInfo);
    int64_t getProcessNum();
    void incProcessNum(int num);
    bool releaseConsumePartition();
#if 0
    bool updatePartitionInfo(PartitionInfo_t &p);
#endif
    bool checkNewOffsetSetting(std::string &topic, std::string &groupId);
    bool checkNeedExit(std::string &consumeInfo);
    int initBrokerConnect(PartitionInfo_t *partInfo);
    int commitPartitionOffset(std::string &topic, std::string &groupId, PartitionInfo_t *partInfo);
    int64_t getOwnerShipAndOffset(std::string &topic, std::string &groupId, std::string &brokerId, int partition);
private:
    KafkaSimpleConsumer* getBrokerConnection(std::string &host, int port);
    static int gc(const char *key, void *data);
};


//} // namepace Qbus

#endif // __CONSUMER_H__

