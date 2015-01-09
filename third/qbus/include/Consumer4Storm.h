#ifndef _Consumer4Storm_H
#define _Consumer4Storm_H


#include "Consumer.h"

#include <string>
#include <vector>


#define DEFAULT_CONF_PATH \
    "/home/infra/qbus/client/cpp/conf/qbus-client.conf"


class Consumer4Storm {
private:
    KafkaConsumer *consumer_;
    PartitionInfo_t *p_;
    std::string topic_;
    std::string group_;
    int errno_;
    bool committed_;
    int i_;
    int partition_no_;
    bool commit_check_;

public:
    Consumer4Storm(std::string& zk_cluster, std::string& topic,
                   std::string& group,
                   const std::string& conf_path = DEFAULT_CONF_PATH);
    ~Consumer4Storm();
    int getErrno() const;
    bool setCommitCheck(bool);
    std::vector<std::string> *nextMessages();
    int nextMessagesNonBlocking(std::vector<std::string>& res);
    PartitionInfo_t *getCurrPartition();
    bool commitOffset();
};


#endif
