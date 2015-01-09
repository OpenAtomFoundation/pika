#ifndef __CONFIG_H__
#define __CONFIG_H__

#include "SimpleHashtab.h"

#include <string>

class KafkaConfig {

    public:
    /*
     * broker信息在ea等缓存中的失效时间
     */
    static int brokerInfoCacheTime_;

    /*
     * 发送慢日志记录时间阀值（毫秒)
     */
    static int slowSendTime_;

    /*
     * 连接broker的超时时间,毫秒为单位
     */
    static int zookeeperTimeout_;

    /*
     * zookeeper receive timeout milliseconds;
     */
    static int zkRecvTimeout_ ;

    /**
     * sleeping microseconds in case of getting empty message from broker
     **/
    static int sleepMicroSec_;

    /**
     * max try times in failure for one message
     **/
    static int maxTryTimes_;

    /**
     * max process message number. if exceed this number, an exception will be thrown. 0 represent no limit
     **/
    static int maxProcessNum_ ;

    /**
     * max process second. An exception will be thrown after running maxProcessSeconds. 0 means no limit
     **/
    static int maxProcessSeconds_;

    /**
     * the tcp connection timeout to broker
     **/
    static int socketTimeout_;

    /**
     *tcp socket buffer size
     **/
    static int socketBufferSize_;

    /**
     * max buffer size to fetch message from broker, it should be bigger than message length + 10 at least
     * */
    static int maxFetchSize_;

    /**
     * if it is true, the consumed offset will be commit to zookeeper each time a message has been consumed
     * if false, the offset will be commit after consuming a batch messages.
     **/
    static bool singleOffsetCommit_;

    /**
     * $var Int the position when want offset not in broker
     * 0    consume from the offset can get
     * 1    consume from the newest
     **/
    static int consumePosition_;

    /**
     * minimal interval(seconds) to poll zookeeper for the new consume configuration.
     **/
    static int pollZKInterval_;

    static void init();
    static std::string getZkClusterHosts(std::string &cluster);
};

#endif //__KAFKA_CONFIG_H__
