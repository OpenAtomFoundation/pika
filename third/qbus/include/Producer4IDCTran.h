#ifndef _PRODUCER_H
#define _PRODUCER_H


#include "ConstDef.h"
#include "HSearchHelper.h"
#include "SimpleConnPool.h"

#include <pthread.h>
#include <stdlib.h>
#include <stdint.h>

#include <string>
#include <vector>
#include <map>


#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    const char *cluster;
    const char *servers;
} zk_cluster_t;

typedef struct {
    // IPv4
    char host[20];
    int port;
} broker_info_t;

#ifdef __cplusplus
}
#endif


class Producer4IDCTran {
private:
    // comma separated host:port pairs, each corresponding to a zk
    // server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
    std::string zk_cluster_;
    // XXX: necessary?
    std::string zk_servers_;

    int errno_;
    std::string errstr_;

    // local file for async send
    std::string async_file_;

    // whether ack from brokers is needed
    bool safe_send_;

    unsigned int msg_max_;
    unsigned int total_max_;
    unsigned int conn_max_;

    struct timeval recv_timeout_;
    struct timeval send_timeout_;

    static bool hsearch_inited_;

    static struct hsearch_data instances_;
    static pthread_mutex_t instance_mutex_;

public:
    // zk cluster --> zk handler
    static hsearch_data cluster_handle_map_;
    static pthread_rwlock_t handle_lock_;

    // broker ids --> connections to brokers
    static hsearch_data broker_conns_;
    static pthread_mutex_t conn_mutex_;

    // broker ids --> pairs of host:port
    static struct hsearch_data brokers_info_;
    static pthread_rwlock_t cache_lock_;

    // topic --> broker id
    static struct hsearch_data topic_broker_map_;

    // topic --> broker id --> partition
    static std::map<std::string, std::map<std::string, int> > topic_part_map_;

private:
    Producer4IDCTran() { }
    Producer4IDCTran(const std::string& zk_cluster,
                  const std::string& zk_servers,
                  bool safe_send = true);
    Producer4IDCTran(const Producer4IDCTran& rh) { (void)rh; }
    virtual ~Producer4IDCTran();

    bool initZk();

    static bool initHSearch(std::string& errstr);

    // portable thread-safe strerror
    static char *strerr_ts(int errnum, char *buf, size_t buflen);

    int takeConn(conn_pool_t *sp, const std::string& broker_id);
    void putConn(const std::string& broker_id, int sockfd);

    int connectBroker(const std::string& broker_id);
    int connect(const std::string& broker_id);
    int sendAll(int sockfd, const std::string& data, int flags);
    int recvAll(int sockfd, void *buf, size_t len, int flags);

    bool getBrokersInfo();
    bool updateTopicNum(std::string& topic);
    uint32_t getMsgsSize(const std::vector<std::string>& messages);

public:
    static Producer4IDCTran *getInstance(const std::string& zk_cluster,
                                      const std::string& conf_path = "/home"
                                 "/infra/qbus/client/cpp/conf/qbus-client.conf",
                                      bool safe_send = true);

    /**
     * @return bool
     * @retval true ack is needed before this set
     * @retval false ack is not needed before this set
     */
    bool setSendAck(bool safe_send = true);

    bool setSendTimeout(int seconds);
    bool setRecvTimeout(int seconds);

    bool setConnMax(int max);

    bool warmup(std::string& topic);

    int calcPartitionTotal(std::string& topic);

    bool getBrokerAndPartition(std::map<std::string, std::string>& brokers_map,
                               const std::string& topic,
                               const std::string& src_broker_id,
                               int src_part,
                               std::string& dest_broker_id,
                               int *dest_part);

    bool asyncSend(std::vector<std::string>& messages,
                   const std::string& topic,
                   std::string& errstr,
                   const std::string& sequence = "");

    bool send(std::vector<std::string>& messages,
              const std::string& topic,
              const std::string& broker_id,
              int partition,
              std::string& errstr,
              uint8_t zflag = 0);

private:
    class Producer4IDCTranGarbage {
    public:
        ~Producer4IDCTranGarbage()
        {
#if 1
            if (instances_.size == 0) return;

            Producer4IDCTran *p;

            for (unsigned int i = 0; i <= instances_.size; i++) {
                if (instances_.table[i].used == 0
                    || instances_.table[i].used == (unsigned int)-1)
                {
                    continue;
                }

                free(instances_.table[i].entry.key);
                p = (Producer4IDCTran *)instances_.table[i].entry.data;
                delete p;
            }

            hdestroy_r(&instances_);

            // more cleanup
            // brokers_info_
            // broker_conns_
            // cluster_handle_map_
            // topic_broker_map_
#endif
        }
    };

    static Producer4IDCTranGarbage garb_;
};


#endif
