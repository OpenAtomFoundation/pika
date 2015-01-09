#ifndef __CONSTDEF_H__
#define __CONSTDEF_H__

#include <sys/types.h>
#include <string>

class KafkaConstDef
{
    public:
    static const int INVALID_PARTITION;
    static const std::string KAFKA_VERSION;
    static const int64_t INVALID_OFFSET;
    static const int MESSAGE_RANDOM_SEND;
    static const int MESSAGE_AFFINITY_SEND;
    static const int MESSAGE_SEMANTIC_SEND;
    static const int MESSAGE_BATCH_SEND;
    static const int RANDOM_PARTITION;
    static const unsigned int MESSAGE_BOUNDARY_MAGIC;
    static const unsigned char MESSAGE_VERSION;
    static const unsigned char SEQUENCE_MESSAGE_VERSION;
    static const std::string CLUSTER_DATA_PREFIX;
    static const int INVALID_SOCKET;
};

#endif // __CONSTDEF_H__
