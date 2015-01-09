#ifndef __ZKUTILS_H__
#define __ZKUTILS_H__

#include <sys/types.h>
#include "zookeeper/zookeeper.h"
#include <string>
#include "SimpleHashtab.h"

class ZkUtils
{
    private:
    static const std::string consumer_dir_;
    static const std::string broker_ids_dir_;
    static const std::string broker_topics_dir_;

    static int get(zhandle_t *handle, std::string &path, char** buf, int *len, struct Stat *s);
    static int create(zhandle_t *handle, const std::string& path, const std::string& data, int flags);
    static int createParentPath(zhandle_t *handle, const std::string& path);

    public:
    static int commitOffset(zhandle_t *handle, std::string &topic, std::string &groupId,
            std::string &brokerId, int partition, int64_t offset);
    static int getOffset(zhandle_t *handle, std::string &topic, std::string &groupId,
            std::string &brokerId, int partition, int64_t *offset);
    static int getConsumeInfo(zhandle_t *handle, std::string &topic, std::string &groupId,
            std::string &consumerId, std::string &info, struct Stat *s);
    static int registerPartitionOwnership(zhandle_t *handle, std::string &topic,
            std::string &groupId, std::string &brokerId, int partition, std::string &consumerId);
    static int releasePartitionOwnership(zhandle_t *handle, std::string &topic, std::string &groupId,
            std::string &brokerId, int partition, std::string &consumerId);
    static int getPartitionNum(zhandle_t *handle, std::string &topic, std::string &brokerId, int *partition);
    static int getAllBrokersId(zhandle_t *handle, struct String_vector *children);
    static int getBrokerIPAndPort(zhandle_t *handle, std::string &brokerId, std::string &ip, int *port);
    static int createEphemeralPath(zhandle_t *handle, const std::string& path, const std::string& data);
    static int createPersistent(zhandle_t *handle, std::string &path, std::string &data);
    static int createEphemeral(zhandle_t *handle, const std::string& path, const std::string& data);
    static int registerConsumeInfo(zhandle_t *handle, std::string &topic, std::string &groupId, std::string &consumerId);
    static int deleteNewOffset(zhandle_t *handle, std::string &topic, std::string &groupId, std::string partition);
    static int getNewOffset(zhandle_t *handle, const std::string& topic, const std::string& groupId,
            Hashtab *offsets);
};

#endif // __ZKUTILS_H__
