#ifndef __PIKA_SERVER_H__
#define __PIKA_SERVER_H__

#include <stdio.h>
#include <sys/epoll.h>
#include <stdlib.h>
#include <fcntl.h>
#include <event.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <ctime>

#include "status.h"
#include "csapp.h"
#include "xdebug.h"
#include "pika_define.h"
#include "nemo.h"
#include "mutexlock.h"
//#include "leveldb/db.h"
//#include "leveldb/write_batch.h"

class PikaThread;
class PikaEpoll;
class PikaConn;

struct SlaveItem {
    std::string ip;
    int64_t port;
    int64_t state;
};

class PikaServer
{
public:
    PikaServer();
    ~PikaServer();

    void RunProcess();

    static void* StartThread(void* arg);
    nemo::Nemo* GetHandle() {return db_;};
    int ClientNum();
    int ClientList(std::string &res);
    int ClientKill(std::string &ip_port);
    void ClientKillAll();
    int GetSlaveList(std::string &res);
//    int ClientRole(int fd, int role);
    
    time_t start_time_s() {return start_time_s_;}
    struct tm *start_time_tm() {return &start_time_tm_;}
    void set_masterhost(std::string &masterhost) { masterhost_ = masterhost; }
    std::string masterhost() { return masterhost_; }
    void set_masterport(int masterport) { masterport_ = masterport; }
    int masterport() { return masterport_; }
    void set_repl_state(int repl_state) { repl_state_ = repl_state; }
    int repl_state() { return repl_state_; }
    port::Mutex* Mutex() { return &mutex_; }
    std::string GetServerIp();
//    void Offline(std::string ip_port);
    int GetServerPort();
    int TrySync(/*std::string &ip, std::string &port,*/ int fd, uint64_t filenum, uint64_t offset);
//    int Slavenum() { return slaves_.size(); }
//    std::map<std::string, SlaveItem>* slaves() { return &slaves_; }
    void ProcessTimeEvent(struct timeval*);
    void DisconnectFromMaster();
    int CurrentQps();
    uint64_t CurrentAccumulativeQueryNums();
    uint64_t HistoryClientsNum();
    
    int repl_state_; //PIKA_SINGLE; PIKA_MASTER; PIKA_SLAVE
    int ms_state_; //PIKA_CONNECT; PIKA_CONNECTED
    pthread_rwlock_t* rwlock() { return &rwlock_; }
    bool LoadDb(std::string& path);
    bool Flushall();
    pthread_t flush_thread_id_;
    bool flushing_;
    static void* StartFlush(void* arg);
    bool purging_;
    bool PurgeLogs(uint32_t max, int64_t to);
    bool PurgeLogsNolock(uint32_t max, int64_t to);
    pthread_t purge_thread_id_;
    static void* StartPurgeLogs(void* arg);
    void AutoPurge();
    struct tm last_autopurge_time_tm_;
    void Slaveofnoone();
    void Dump();
    bool Dumpoff();
    uint32_t dump_filenum_;
    uint64_t dump_pro_offset_;
    pthread_t dump_thread_id_;
    char dump_time_[32];
    static void* StartDump(void* arg);
    nemo::Snapshots snapshot_;
    bool bgsaving_;
    time_t bgsaving_start_time_;
    std::string is_bgsaving();
    bool is_readonly_;

    pthread_t info_keyspace_thread_id_;
    bool info_keyspacing_;
    time_t info_keyspace_start_time_;
    time_t last_info_keyspace_start_time_;
    std::vector<uint64_t> keynums_;
    uint64_t last_kv_num_;
    uint64_t last_hash_num_;
    uint64_t last_list_num_;
    uint64_t last_zset_num_;
    uint64_t last_set_num_;
    std::string is_scaning();
    void InfoKeySpace();
    static void* StartInfoKeySpace(void* arg);

private:
    friend class PikaConn;
    Status SetBlockType(BlockType type);

    /*
     * The udp server port and address
     */
    int sockfd_;
    int slave_sockfd_;
    int flags_;
    int port_;
    struct sockaddr_in servaddr_;

    std::string masterhost_;
    int masterport_;

    port::Mutex mutex_;
//    std::map<std::string, SlaveItem> slaves_;
    /*
     * The Epoll event handler
     */
    PikaEpoll *pikaEpoll_;

    /*
     * The leveldb handler
     */
    nemo::Nemo *db_;
    pthread_rwlock_t rwlock_; // use to block other command for loaddb, dump and readonly
 //   leveldb::DB *db_;

 //   leveldb::Options options_;

    /*
     * Here we used auto poll to find the next work thread,
     * last_thread_ is the last work thread
     */
    int last_thread_;
    int last_slave_thread_;
    int thread_num_;
    /*
     * This is the work threads
     */
    PikaThread *pikaThread_[PIKA_THREAD_NUM];

    int64_t history_clients_num_;
    time_t start_time_s_;
    struct tm start_time_tm_;
//    int64_t stat_numcommands;
//    int64_t stat_numconnections;
//
//    int64_t stat_keyspace_hits;
//    int64_t stat_keyspace_misses;

    // No copying allowed
    PikaServer(const PikaServer&);
    void operator=(const PikaServer&);

};

#endif
