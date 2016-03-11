#include <algorithm>
#include <glog/logging.h>
#include <map>
#include <sys/utsname.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/dir.h>
#include <ctime>

#include "pika_command.h"
#include "pika_server.h"
#include "pika_conf.h"
#include "pika_define.h"
#include "util.h"
#include "mario.h"
#include "mario_item.h"

extern PikaServer *g_pikaServer;
extern mario::Mario *g_pikaMario;
extern std::map<std::string, Cmd *> g_pikaCmd;
extern PikaConf* g_pikaConf;

void AuthCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string password = argv.front();
    argv.pop_front();

    std::string root_password(g_pikaConf->requirepass());
    std::string user_password(g_pikaConf->userpass());
    if (user_password.empty() && root_password.empty()) {
        ret = "-ERR Client sent AUTH, but no password is set\r\n";
        return;
    }

    ret.clear();
    if (password == user_password) {
        ret = "USER";
    }
    if (password == root_password) {
        ret = "ROOT";
    }

    if (ret.empty()) {
        ret = "-ERR invalid password\r\n";
    }
}

void SlaveauthCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string password = argv.front();
    argv.pop_front();

    std::string root_password(g_pikaConf->requirepass());
    std::string user_password(g_pikaConf->userpass());
    if (user_password.empty() && root_password.empty()) {
        ret = "-ERR Client sent AUTH, but no password is set\r\n";
        return;
    }

    ret.clear();
    if (password == user_password) {
        ret = "USER";
    }
    if (password == root_password) {
        ret = "ROOT";
    }

    if (ret.empty()) {
        ret = "-ERR invalid password\r\n";
    }
}

void PingCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    LOG(INFO)<<"Receive Ping";
    ret = "+PONG\r\n";
}

void ClientCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((argv.size() != 2 && argv.size() != 3) || (arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string opt = argv.front();
    argv.pop_front();
    transform(opt.begin(), opt.end(), opt.begin(), ::tolower);
    if (opt == "list") {
        g_pikaServer->ClientList(ret);
    } else if (opt == "kill") {
        std::string ip_port = argv.front();
        argv.pop_front();
        transform(ip_port.begin(), ip_port.end(), ip_port.begin(), ::tolower);
        if (ip_port == "all") {
            g_pikaServer->ClientKillAll();
            ret = "+OK\r\n";
        } else {
            int res = g_pikaServer->ClientKill(ip_port);
            if (res == 1) {
                ret = "+OK\r\n";
            } else {
                ret = "-ERR No such client\r\n";
            }
        }
        
    } else {
        ret = "-ERR Syntax error, try CLIENT (LIST | KILL ip:port)\r\n"; 
    }

}

//void BemasterCmd::Do(std::list<std::string> &argv, std::string &ret) {
//    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
//        ret = "-ERR wrong number of arguments for ";
//        ret.append(argv.front());
//        ret.append(" command\r\n");
//        return;
//    }
//    argv.pop_front();
//    std::string str_fd = argv.front();
//    argv.pop_front();
//    int64_t fd;
//    if (!string2l(str_fd.data(), str_fd.size(), &fd)) {
//        ret = "-ERR value is not an integer or out of range\r\n";
//        return;
//    }
//
//    int res = g_pikaServer->ClientRole(fd, CLIENT_MASTER);
//    if (res == 1) {
//        ret = "+OK\r\n";
//    } else {
//        ret = "-ERR No such client\r\n";
//    }
//}


void SlaveofCmd::Do(std::list<std::string> &argv, std::string &ret) {
    int as = argv.size();
    if ((as != 3 && as != 5) || (arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string p2 = argv.front();
    argv.pop_front();
    std::string p3 = argv.front();
    argv.pop_front();
    if (p2 == "no" && p3 == "one") {
        g_pikaServer->Slaveofnoone();
        ret = "+OK\r\n";
        return;
    }
    int64_t port = 0;
    if (!string2l(p3.data(), p3.size(), &port)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    
    std::string hostip = g_pikaServer->GetServerIp();
    if ((p2 == hostip || p2 == "127.0.0.1") && port == g_pikaServer->GetServerPort()) {
        ret = "-ERR you fucked up\r\n";
        return;
    }

    int64_t filenum = 0;
    int64_t pro_offset = 0;
    bool is_psync = false;
    if (argv.size() == 2) {
        std::string p4 = argv.front();
        argv.pop_front();
        if (!string2l(p4.data(), p4.size(), &filenum)) {
            ret = "-ERR value is not an integer or out of range\r\n";
            return;
        }
        
        std::string p5 = argv.front();
        argv.pop_front();
        if (!string2l(p5.data(), p5.size(), &pro_offset)) {
            ret = "-ERR value is not an integer or out of range\r\n";
            return;
        }
        if (filenum < 0 || pro_offset < 0 || pro_offset > mario::kMmapSize) {
            ret = "-ERR Invalid Offset\r\n";
            return;
        }
        is_psync = true;
    }

    {
    MutexLock l(g_pikaServer->Mutex());
    if (g_pikaServer->masterhost() == p2 && g_pikaServer->masterport() == port) {
        ret = "+OK Already connected to specified master\r\n";
        return;
    } else if ((g_pikaServer->repl_state() & PIKA_SLAVE) != 0) { 
        ret = "-ERR State is already PIKA_SLAVE\r\n";
        return;
    }

    if (g_pikaServer->ms_state_ == PIKA_REP_SINGLE) {
        if (is_psync) {
            g_pikaServer->PurgeLogsNolock(filenum, filenum);
            g_pikaMario->SetProducerStatus(filenum, pro_offset);
        }
        g_pikaServer->ms_state_ = PIKA_REP_CONNECT;
        g_pikaServer->set_masterhost(p2);
        g_pikaServer->set_masterport(port+100);
        ret = "+OK\r\n";

        std::string slave_db_sync_path = g_pikaConf->slave_db_sync_path();
        if ((slave_db_sync_path.size() == 1 && slave_db_sync_path == ".") || (slave_db_sync_path.size() > 1 && slave_db_sync_path.substr(0, 2) == "./")) {
            char cur_word_path[100];
            getcwd(cur_word_path, sizeof(cur_word_path));
            if (*(cur_word_path+strlen(cur_word_path)-1) == '/') {
                *(cur_word_path+strlen(cur_word_path)-1) = '\0';
            }
            slave_db_sync_path = std::string(cur_word_path, strlen(cur_word_path)) + slave_db_sync_path.substr(1);
        }
        if (slave_db_sync_path.back() == '/') {
            slave_db_sync_path.erase(slave_db_sync_path.size()-1);
        }
        //delete_dir(slave_db_sync_path.c_str());
        //struct stat file_info;
        //if (stat(slave_db_sync_path.c_str(), &file_info) == 0) 
        /*
        if (access(slave_db_sync_path.c_str(), F_OK) == 0) {
            char tmp_dir_path[100];
            strncpy(tmp_dir_path, slave_db_sync_path.c_str(), slave_db_sync_path.size()+1);
            strcat(tmp_dir_path, "_tmp");
            if (access(tmp_dir_path, F_OK) == 0) {
                delete_dir(tmp_dir_path);
            }
            rename(slave_db_sync_path.c_str(), tmp_dir_path);
            pthread_t thread_id;
            char *arg = strdup(tmp_dir_path);
            if (pthread_create(&thread_id, NULL, remove_dir, arg)) {
                free(arg);
            }
        }
        */

        mkpath((slave_db_sync_path+"/"+nemo::KV_DB).c_str(), 0755);
        mkpath((slave_db_sync_path+"/"+nemo::HASH_DB).c_str(), 0755);
        mkpath((slave_db_sync_path+"/"+nemo::LIST_DB).c_str(), 0755);
        mkpath((slave_db_sync_path+"/"+nemo::SET_DB).c_str(), 0755);
        mkpath((slave_db_sync_path+"/"+nemo::ZSET_DB).c_str(), 0755);
        //size_t last_slash_pos = slave_db_sync_path.find_last_of("/");
        //std::string slave_db_sync_path_up = slave_db_sync_path.substr(0, last_slash_pos);
        // start_rsync(slave_db_sync_path_up);
        start_rsync(slave_db_sync_path, g_pikaConf->port() + 300);
    } else {
        ret = "-ERR State is not in PIKA_REP_SINGLE\r\n";
    }

    }
}

void PikasyncCmd::Do(std::list<std::string> &argv, std::string &ret) {
    LOG(WARNING) << "receive pikasync command";
    //pthread_rwlock_unlock(g_pikaServer->rwlock());
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
//    std::string ip = argv.front();
//    argv.pop_front();
//    std::string str_port = argv.front();
//    argv.pop_front();
//    int64_t port = 0;
//    if (!string2l(str_port.data(), str_port.size(), &port)) {
//        ret = "-ERR value is not an integer or out of range\r\n";
//        return;
//    }
    std::string str_filenum = argv.front();
    argv.pop_front();
    int64_t filenum = 0;
    if (!string2l(str_filenum.data(), str_filenum.size(), &filenum)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    std::string str_offset = argv.front();
    argv.pop_front();
    int64_t offset = 0;
    if (!string2l(str_offset.data(), str_offset.size(), &offset)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }

    std::string str_dbsyncpath = argv.front();
    argv.pop_front();

    std::string str_rsyncport = argv.front();
    argv.pop_front();
    int64_t rsync_port = 0;
    if (!string2l(str_rsyncport.data(), str_rsyncport.size(), &rsync_port)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }

    std::string str_fd = argv.front();
    argv.pop_front();
    int64_t fd = 0;
    if (!string2l(str_fd.data(), str_fd.size(), &fd)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }

//    LOG(INFO) << ip << " : " << str_port;
    int res = g_pikaServer->TrySync(/*ip, str_port, */fd, filenum, offset);
    std::string t_ret;
    int try_sync_db_ret = -1;
    if (res == PIKA_REP_STRATEGY_PSYNC) {
        t_ret = "*1\r\n$9\r\nucanpsync\r\n";
    } else if (res == PIKA_REP_STRATEGY_MISS) {
        try_sync_db_ret = g_pikaServer->TrySyncDB(str_dbsyncpath, rsync_port, fd);
        if (try_sync_db_ret == 0) {
            t_ret = "*2\r\n$6\r\nsyncdb\r\n$5\r\nstart\r\n";
        } else {
            t_ret = "*1\r\n$9\r\nsyncerror\r\n";
        }
    } else {
        t_ret = "*1\r\n$9\r\nsyncerror\r\n";
    }
    std::map<std::string, std::pair<PikaConn*, std::string> >* syncing_db_slaves = g_pikaServer->syncing_db_slaves();
    if (!(res == PIKA_REP_STRATEGY_MISS && try_sync_db_ret == 0)) {//if the last pikasync's execution is db sync, than this pikasync reset the db_sync_purge_max
        MutexLock lm(g_pikaServer->Mutex());
        if (syncing_db_slaves->empty()) { //there is no db_syncing slaves
            g_pikaServer->set_db_sync_purge_max(-1);
        }
    }
    std::string auth = g_pikaConf->requirepass();
    if (auth.size() == 0) {
        ret = t_ret;
    } else {
        ret = "*2\r\n$4\r\nauth\r\n";
        char buf_len[32];
        snprintf(buf_len, sizeof(buf_len), "$%d\r\n", auth.size());
        ret.append(buf_len);
        ret.append(auth);
        ret.append("\r\n");
        ret.append(t_ret);
    }
    LOG(WARNING) << ret;
}

void UcanpsyncCmd::Do(std::list<std::string> &argv, std::string &ret) {
    pthread_rwlock_unlock(g_pikaServer->rwlock());
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();

    {
    MutexLock l(g_pikaServer->Mutex());
    if (g_pikaServer->ms_state_ == PIKA_REP_CONNECTING) {
        g_pikaServer->ms_state_ = PIKA_REP_CONNECTED;
    }
    }

    {
    RWLock rwl(g_pikaServer->rwlock(), true);
    g_pikaServer->is_readonly_ = true;
    }
    g_pikaConf->SetReadonly(true);
    LOG(WARNING) << "Master told me that I can psync, open readonly mode now";
    ret = "";
    std::string slave_db_sync_path = g_pikaConf->slave_db_sync_path();
    if (access(slave_db_sync_path.c_str(), F_OK) == 0) {
        delete_dir(slave_db_sync_path.c_str());
    }
    stop_rsync(g_pikaConf->slave_db_sync_path());
}

void SyncdbCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((int)argv.size() < 2) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    /*
    {
        MutexLock l(g_pikaServer->Mutex());
        if (g_pikaServer->ms_state_ == PIKA_REP_CONNECTING) {
            g_pikaServer->ms_state_ = PIKA_REP_CONNECTED;
        }
    }
    */

    std::string slave_db_sync_path = g_pikaConf->slave_db_sync_path();
    if ((slave_db_sync_path.size() == 1 && slave_db_sync_path == ".") || (slave_db_sync_path.size() > 1 && slave_db_sync_path.substr(0, 2) == "./")) {
        char cur_word_path[100];
        getcwd(cur_word_path, sizeof(cur_word_path));
        if (*(cur_word_path+strlen(cur_word_path)-1) == '/') {
            *(cur_word_path+strlen(cur_word_path)-1) = '\0';
        }
        slave_db_sync_path = std::string(cur_word_path, strlen(cur_word_path)) + slave_db_sync_path.substr(1);
    }
    if (argv.front() == "start" && argv.size() == 1) {
        ret = "";
    } else if (argv.front() == "finished" && argv.size() == 3) {
        int64_t pro_filenum;
        argv.pop_front();
        std::string pro_filenum_str = argv.front();
        if (!string2l(pro_filenum_str.data(), pro_filenum_str.size(), &pro_filenum)) {
            ret = "-ERR value is not an integer or out of range\r\n";
            return;
        }
        argv.pop_front();
        std::string pro_offset_str = argv.front();
        int64_t pro_offset;
        if (!string2l(pro_offset_str.data(), pro_offset_str.size(), &pro_offset)) {
            ret = "-ERR Invalid offset";
            return;
        }
        pthread_rwlock_unlock(g_pikaServer->rwlock());
        g_pikaServer->ReloadDb(slave_db_sync_path);
        pthread_rwlock_rdlock(g_pikaServer->rwlock());
        {
            MutexLock l(g_pikaServer->Mutex());
            g_pikaServer->purging_ = true;
            remove_files(g_pikaConf->log_path(), "write2file");
            g_pikaServer->purging_ = false;
        }
        g_pikaMario->SetProducerStatus(pro_filenum, pro_offset);

        std::string auth = g_pikaConf->requirepass();
        char buf_len[32];
        if (auth.size() == 0) {
            ret.assign("*5\r\n$8\r\npikasync\r\n");
        } else {
            ret.assign("*2\r\n$9\r\nslaveauth\r\n");
            snprintf(buf_len, sizeof(buf_len), "$%d\r\n", auth.size());
            ret.append(buf_len);
            ret.append(auth);
            ret.append("\r\n");
            ret.append("*5\r\n$8\r\npikasync\r\n");
        }
        snprintf(buf_len, sizeof(buf_len), "$%d\r\n", pro_filenum_str.size());
        ret.append(buf_len);
        ret.append(pro_filenum_str + "\r\n");
        snprintf(buf_len, sizeof(buf_len), "$%d\r\n", pro_offset_str.size());
        ret.append(buf_len);
        ret.append(pro_offset_str + "\r\n");
        //snprintf(buf_len, sizeof(buf_len), "$%d\r\n", slave_db_sync_path.size());
        //ret.append(slave_db_sync_path + "\r\n");

        char pid_str[10];
        snprintf(pid_str, sizeof(pid_str), "%d", getpid());
        std::string pika_slave_db_sync_path = "pika_slave_db_sync_path_";
        pika_slave_db_sync_path.append(pid_str);
        snprintf(buf_len, sizeof(buf_len), "$%d\r\n", pika_slave_db_sync_path.size());
        ret.append(buf_len);
        ret.append(pika_slave_db_sync_path + "\r\n");

        uint32_t rsync_port = g_pikaConf->port() + 300;
        char rsync_port_str[10];
        snprintf(rsync_port_str, sizeof(rsync_port_str), "%d", rsync_port);
        snprintf(buf_len, sizeof(buf_len), "$%d\r\n", strlen(rsync_port_str));
        ret.append(buf_len);
        ret.append(rsync_port_str);
        ret.append("\r\n");

        LOG(WARNING) << ret;

        mkpath((slave_db_sync_path+"/"+nemo::KV_DB).c_str(), 0755);
        mkpath((slave_db_sync_path+"/"+nemo::HASH_DB).c_str(), 0755);
        mkpath((slave_db_sync_path+"/"+nemo::LIST_DB).c_str(), 0755);
        mkpath((slave_db_sync_path+"/"+nemo::SET_DB).c_str(), 0755);
        mkpath((slave_db_sync_path+"/"+nemo::ZSET_DB).c_str(), 0755);
    } else {
        ret = "-ERR wrong argument for";
        ret.append(argv.front());
        ret.append(" command\r\n");
    }
}

void PutInt32(std::string *dst, int32_t v) {
    std::string vstr = std::to_string(v);
    dst->append(vstr);
}

void SyncerrorCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    g_pikaServer->DisconnectFromMaster();
    LOG(WARNING) << "Master told me that I can not psync, rollback now";
    ret = "";
    std::string slave_db_sync_path = g_pikaConf->slave_db_sync_path();
    if (access(slave_db_sync_path.c_str(), F_OK) == 0) {
        delete_dir(slave_db_sync_path.c_str());
    }
    stop_rsync(slave_db_sync_path);
}

void LoaddbCmd::Do(std::list<std::string> &argv, std::string &ret) {
    pthread_rwlock_unlock(g_pikaServer->rwlock());
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string path = argv.front();
    argv.pop_front();
    bool res = g_pikaServer->LoadDb(path);
    if (res == true) {
        ret = "+OK\r\n";
    } else {
        ret = "-ERR Load DB Error\r\n";
    }
}

void FlushallCmd::Do(std::list<std::string> &argv, std::string &ret) {
    pthread_rwlock_unlock(g_pikaServer->rwlock());
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    bool res = g_pikaServer->Flushall();
    if (res == true) {
        ret = "+OK\r\n";
    } else {
        ret = "-ERR Alread in Flushing\r\n";
    }
}

void ShutdownCmd::Do(std::list<std::string> &argv, std::string &ret) {
    pthread_rwlock_unlock(g_pikaServer->rwlock());
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();

    std::string hostip = g_pikaServer->GetServerIp();

    //printf ("Shutdown set shutdown to true\n");
    g_pikaServer->shutdown = true;
// if (res == true) {
//        ret = "\r\n";
//    } else {
//        ret = "-ERR Alread in Flushing\r\n";
//    }
}

void DumpCmd::Do(std::list<std::string> &argv, std::string &ret) {
    pthread_rwlock_unlock(g_pikaServer->rwlock());
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    g_pikaServer->Dump();
    ret = "+";
    ret.append(g_pikaServer->dump_time_);
    ret.append(" : ");
    char buf[32];
    snprintf(buf, sizeof(buf), "%d", g_pikaServer->dump_filenum_);
    ret.append(buf);
    ret.append(" : ");
    snprintf(buf, sizeof(buf), "%lu", g_pikaServer->dump_pro_offset_);
    ret.append(buf);
    ret.append("\r\n");
}

void DumpoffCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    bool res = g_pikaServer->Dumpoff();
    if (res == true) {
        ret = "+OK\r\n";
    } else {
        ret = "-ERR No DumpWorks now\r\n";
    }
}

void ReadonlyCmd::Do(std::list<std::string> &argv, std::string &ret) {
    pthread_rwlock_unlock(g_pikaServer->rwlock());
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string opt = argv.front();
    argv.pop_front();
    transform(opt.begin(), opt.end(), opt.begin(), ::tolower);
    ret = "+OK\r\n";
    if (opt == "on") {
        {
        RWLock l(g_pikaServer->rwlock(), true);
        MutexLock lm(g_pikaServer->Mutex());
        g_pikaServer->is_readonly_ = true;
        g_pikaConf->SetReadonly(true);
        }
    } else if (opt == "off") {
        {
        RWLock l(g_pikaServer->rwlock(), true);
        MutexLock lm(g_pikaServer->Mutex());
        g_pikaServer->is_readonly_ = false;
        g_pikaConf->SetReadonly(false);
        }
    } else {
        ret = "-ERR invalid option for readonly(on/off?)\r\n";
    }
}

void SelectCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string str_db_num = argv.front();
    argv.pop_front();
    int64_t db_num = 0;
    if (!string2l(str_db_num.data(), str_db_num.size(), &db_num)) {
        ret = "-ERR value is not an integer or out of range\r\n";
        return;
    }
    ret = "+OK\r\n";
}

void EncodeString(std::string *dst, const std::string &value) {
    dst->append("$");
    PutInt32(dst, value.size());
    dst->append("\r\n");
    dst->append(value.data(), value.size());
    dst->append("\r\n");
}

void EncodeInt32(std::string *dst, const int32_t v) {
    std::string vstr = std::to_string(v);
    dst->append("$");
    PutInt32(dst, vstr.length());
    dst->append("\r\n");
    dst->append(vstr);
    dst->append("\r\n");
}

void ConfigCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if (argv.size() == 2) {
        std::string opt = argv.back();
        transform(opt.begin(), opt.end(), opt.begin(), ::tolower);
        if (opt != "rewrite") {
            ret = "-ERR wrong number of arguments for 'config' command\r\n";
            return;
        }
        int ret_i = g_pikaConf->ConfigRewrite();
        if (ret_i == 0) {
            ret = "+OK\r\n";
        } else if (ret_i == -1) {
            ret = "-ERR open new conf file error\r\n";
        } else if (ret_i == -2) {
            ret = "-ERR rename new conf file name to origin conf file error\r\n";
        } else {
            ret = "-ERR config rewrite failed";
        }
        return;
    }
    if (argv.size() != 3 && argv.size() != 4) {
        ret = "-ERR wrong number of arguments for 'config' command\r\n";
        return;
    }
    argv.pop_front();
    std::string opt = argv.front();
    argv.pop_front();
    transform(opt.begin(), opt.end(), opt.begin(), ::tolower);

    std::string conf_item = argv.front();
    argv.pop_front();
    transform(conf_item.begin(), conf_item.end(), conf_item.begin(), ::tolower);
  
    if (argv.size() == 0 && opt == "get") {
        if (conf_item == "port") {
            ret = "*2\r\n";
            EncodeString(&ret, "port");
            EncodeInt32(&ret, g_pikaConf->port());
        } else if (conf_item == "thread_num") {
            ret = "*2\r\n";
            EncodeString(&ret, "thread_num");
            EncodeInt32(&ret, g_pikaConf->thread_num());
        } else if (conf_item == "slave_thread_num") {
            ret = "*2\r\n";
            EncodeString(&ret, "slave_thread_num");
            EncodeInt32(&ret, g_pikaConf->slave_thread_num());
        } else if (conf_item == "log_path") {
            ret = "*2\r\n";
            EncodeString(&ret, "log_path");
            EncodeString(&ret, g_pikaConf->log_path());
        } else if (conf_item == "log_level") {
            ret = "*2\r\n";
            EncodeString(&ret, "log_level");
            EncodeInt32(&ret, g_pikaConf->log_level());
        } else if (conf_item == "db_path") {
            ret = "*2\r\n";
            EncodeString(&ret, "db_path");
            EncodeString(&ret, g_pikaConf->db_path());
        } else if (conf_item == "maxmemory") {
            ret = "*2\r\n";
            EncodeString(&ret, "maxmemory");
            EncodeInt32(&ret, g_pikaConf->write_buffer_size());
        } else if (conf_item == "write_buffer_size") {
            ret = "*2\r\n";
            EncodeString(&ret, "write_buffer_size");
            EncodeInt32(&ret, g_pikaConf->write_buffer_size());
        } else if (conf_item == "timeout") {
            ret = "*2\r\n";
            EncodeString(&ret, "timeout");
            EncodeInt32(&ret, g_pikaConf->timeout());
        } else if (conf_item == "requirepass") {
            ret = "*2\r\n";
            EncodeString(&ret, "requirepass");
            EncodeString(&ret, g_pikaConf->requirepass());
        } else if (conf_item == "userpass") {
            ret = "*2\r\n";
            EncodeString(&ret, "userpass");
            EncodeString(&ret, g_pikaConf->userpass());
        } else if (conf_item == "userblacklist") {
            ret = "*2\r\n";
            EncodeString(&ret, "userblacklist");
            EncodeString(&ret, (g_pikaConf->suser_blacklist()).c_str());
        } else if (conf_item == "dump_prefix") {
            ret = "*2\r\n";
            EncodeString(&ret, "dump_prefix");
            EncodeString(&ret, g_pikaConf->dump_prefix());
        } else if (conf_item == "daemonize") {
            ret = "*2\r\n";
            EncodeString(&ret, "daemonize");
            EncodeString(&ret, g_pikaConf->daemonize() ? "yes" : "no");
        } else if (conf_item == "dump_path") {
            ret = "*2\r\n";
            EncodeString(&ret, "dump_path");
            EncodeString(&ret, g_pikaConf->dump_path());
        } else if (conf_item == "pidfile") {
            ret = "*2\r\n";
            EncodeString(&ret, "pidfile");
            EncodeString(&ret, g_pikaConf->pidfile());
        } else if (conf_item == "maxconnection") {
            ret = "*2\r\n";
            EncodeString(&ret, "maxconnection");
            EncodeInt32(&ret, g_pikaConf->maxconnection());
        } else if (conf_item == "target_file_size_base") {
            ret = "*2\r\n";
            EncodeString(&ret, "target_file_size_base");
            EncodeInt32(&ret, g_pikaConf->target_file_size_base());
        } else if (conf_item == "expire_logs_days") {
            ret = "*2\r\n";
            EncodeString(&ret, "expire_logs_days");
            EncodeInt32(&ret, g_pikaConf->expire_logs_days());
        } else if (conf_item == "expire_logs_nums") {
            ret = "*2\r\n";
            EncodeString(&ret, "expire_logs_nums");
            EncodeInt32(&ret, g_pikaConf->expire_logs_nums());
        } else if (conf_item == "root_connection_num" ) {
            ret = "*2\r\n";
            EncodeString(&ret, "root_connection_num");
            EncodeInt32(&ret, g_pikaConf->root_connection_num());
        } else if (conf_item == "slowlog_log_slower_than") {
            ret = "*2\r\n";
            EncodeString(&ret, "slowlog_log_slower_than");
            EncodeInt32(&ret, g_pikaConf->slowlog_slower_than());
        } else if (conf_item == "binlog_file_size") {
            ret = "*2\r\n";
            EncodeString(&ret, "binlog_file_size");
            EncodeInt32(&ret, g_pikaConf->binlog_file_size());
        } else if (conf_item == "compression") {
            ret = "*2\r\n";
            EncodeString(&ret, "compression");
            EncodeString(&ret, g_pikaConf->compression());
        } else if (conf_item == "slave-read-only") {
          ret = "*1\r\n";
          if (!g_pikaServer->is_readonly_) {
            EncodeString(&ret, "0");
          } else {
            EncodeString(&ret, "1");
          }
        } else if (conf_item == "master_db_sync_path") {
            ret = "*2\r\n";
            EncodeString(&ret, "master_db_sync_path");
            EncodeString(&ret, g_pikaConf->master_db_sync_path());
        } else if (conf_item == "slave_db_sync_path") {
            ret = "*2\r\n";
            EncodeString(&ret, "slave_db_sync_path");
            EncodeString(&ret, g_pikaConf->slave_db_sync_path());
        } else if (conf_item == "db_sync_speed") {
            ret = "*2\r\n";
            EncodeString(&ret, "db_sync_speed");
            EncodeInt32(&ret, g_pikaConf->db_sync_speed());
        } else if (conf_item == "*") {
          ret = "*28\r\n";
          EncodeString(&ret, "port");
          EncodeString(&ret, "thread_num");
          EncodeString(&ret, "slave_thread_num");
          EncodeString(&ret, "log_path");
          EncodeString(&ret, "log_level");
          EncodeString(&ret, "db_path");
          EncodeString(&ret, "maxmemory");
          EncodeString(&ret, "write_buffer_size");
          EncodeString(&ret, "timeout");
          EncodeString(&ret, "requirepass");
          EncodeString(&ret, "userpass");
          EncodeString(&ret, "userblacklist");
          EncodeString(&ret, "dump_prefix");
          EncodeString(&ret, "daemonize");
          EncodeString(&ret, "dump_path");
          EncodeString(&ret, "pidfile");
          EncodeString(&ret, "maxconnection");
          EncodeString(&ret, "target_file_size_base");
          EncodeString(&ret, "expire_logs_days");
          EncodeString(&ret, "expire_logs_nums");
          EncodeString(&ret, "root_connection_num");
          EncodeString(&ret, "slowlog_log_slower_than");
          EncodeString(&ret, "slave-read-only");
          EncodeString(&ret, "binlog_file_size");
          EncodeString(&ret, "compression");
          EncodeString(&ret, "master_db_sync_path");
          EncodeString(&ret, "slave_db_sync_path");
          EncodeString(&ret, "db_sync_speed");
        } else {
            ret = "-ERR No such configure item\r\n";
        }
    } else if (argv.size() == 1 && opt == "set") {
        std::string value = argv.front();
        long int ival;
        if (conf_item == "log_level") {
            if (!string2l(value.data(), value.size(), &ival) || ival < 0 || ival > 4) {
                ret = "-ERR Invalid argument " + value + " for CONFIG SET 'log_level'\r\n";
                return;
            }
            g_pikaConf->SetLogLevel(ival);
            FLAGS_minloglevel = g_pikaConf->log_level();
            ret = "+OK\r\n";
        } else if (conf_item == "timeout") {
            if (!string2l(value.data(), value.size(), &ival)) {
                ret = "-ERR Invalid argument " + value + " for CONFIG SET 'timeout'\r\n";
                return;
            }
            g_pikaConf->SetTimeout(ival);
            ret = "+OK\r\n";
        } else if (conf_item == "requirepass") {
            g_pikaConf->SetRequirePass(value);
            ret = "+OK\r\n";
        } else if (conf_item == "userpass") {
            g_pikaConf->SetUserPass(value);
            ret = "+OK\r\n";
        } else if (conf_item == "userblacklist") {
            g_pikaConf->SetUserBlackList(value);
            ret = "+OK\r\n";
        } else if (conf_item == "dump_prefix") {
            g_pikaConf->SetDumpPrefix(value);
            ret = "+OK\r\n";
        } else if (conf_item == "maxconnection") {
            if (!string2l(value.data(), value.size(), &ival)) {
                ret = "-ERR Invalid argument " + value + " for CONFIG SET 'maxconnection'\r\n";
                return;
            }
            g_pikaConf->SetMaxConnection(ival);
            ret = "+OK\r\n";
        } else if (conf_item == "expire_logs_days") {
            if (!string2l(value.data(), value.size(), &ival)) {
                ret = "-ERR Invalid argument " + value + " for CONFIG SET 'expire_logs_days'\r\n";
                return;
            }
            g_pikaConf->SetExpireLogsDays(ival);
            ret = "+OK\r\n";
        } else if (conf_item == "expire_logs_nums") {
            if (!string2l(value.data(), value.size(), &ival)) {
                ret = "-ERR Invalid argument " + value + " for CONFIG SET 'expire_logs_nums'\r\n";
                return;
            }
            g_pikaConf->SetExpireLogsNums(ival);
            ret = "+OK\r\n";
        } else if (conf_item == "root_connection_num") {
            if (!string2l(value.data(), value.size(), &ival)) {
                ret = "-ERR Invalid argument " + value + " for CONFIG SET 'root_connection_num'\r\n";
                return;
            }
            g_pikaConf->SetRootConnectionNum(ival);
            ret = "+OK\r\n";
        } else if (conf_item == "slowlog_log_slower_than") {
            if (!string2l(value.data(), value.size(), &ival)) {
                ret = "-ERR Invalid argument " + value + " for CONFIG SET 'slowlog_slower_than'\r\n";
                return;
            }
            g_pikaConf->SetSlowlogSlowerThan(ival);
            ret = "+OK\r\n";
        } else if (conf_item == "slave-read-only") {
            transform(value.begin(), value.end(), value.begin(), ::tolower);
            bool is_readonly;
            if (value == "1" || value == "yes") {
                is_readonly = true;
            } else if (value == "0" || value == "no") {
                is_readonly = false;
            } else {
                ret = "-ERR Invalid argument " + argv.front() + " for CONFIG SET 'readonly'\r\n";
                return;
            }
            pthread_rwlock_unlock(g_pikaServer->rwlock());
            {
            RWLock l(g_pikaServer->rwlock(), true);
            g_pikaServer->is_readonly_ = is_readonly;
            }
            g_pikaConf->SetReadonly(is_readonly);
            pthread_rwlock_rdlock(g_pikaServer->rwlock());
            ret = "+OK\r\n";
        } else if (conf_item == "master_db_sync_path") {
            g_pikaConf->SetMasterDbSyncPath(value);
            ret = "+OK\r\n";
        } else if (conf_item == "slave_db_sync_path") {
            g_pikaConf->SetSlaveDbSyncPath(value);
            ret = "+OK\r\n";
        } else if (conf_item == "db_sync_speed") {
            if (!string2l(value.data(), value.size(), &ival)) {
                ret = "-ERR Invalid argument " + value + " for CONFIG SET 'db_sync_speed(MB)'\r\n";
                return;
            }
            if (ival < 0 || ival > 125) {
                ival = 125;
            }
            g_pikaConf->SetDbSyncSpeed(ival);
            ret = "+OK\r\n";
        } else {
            ret = "-ERR No such configure item\r\n";
        }
    } else if (argv.size() == 0 && opt == "set" && conf_item == "*") {
        ret = "*15\r\n";
        EncodeString(&ret, "log_level");
        EncodeString(&ret, "timeout");
        EncodeString(&ret, "requirepass");
        EncodeString(&ret, "userpass");
        EncodeString(&ret, "userblacklist");
        EncodeString(&ret, "dump_prefix");
        EncodeString(&ret, "maxconnection");
        EncodeString(&ret, "expire_logs_days");
        EncodeString(&ret, "expire_logs_nums");
        EncodeString(&ret, "root_connection_num");
        EncodeString(&ret, "slowlog_log_slower_than");
        EncodeString(&ret, "slave-read-only");
        EncodeString(&ret, "master_db_sync_path");
        EncodeString(&ret, "slave_db_sync_path");
        EncodeString(&ret, "db_sync_speed");
    } else {
        ret = "-ERR wrong number of arguments for CONFIG ";
        ret.append(opt);
        ret.append(" command\r\n");
        return;
    }
}

unsigned long long du(const std::string& filename)
{
	struct stat statbuf;
	unsigned long long sum;
    if (lstat(filename.c_str(), &statbuf) != 0) {
		return 0;
	}

	// sum = statbuf.st_blocks * statbuf.st_blksize;
	sum = statbuf.st_size;
	// cout << filename << ": " << sum << " bytes" << endl;

	if (S_ISLNK(statbuf.st_mode)) {
		if (stat(filename.c_str(), &statbuf) != 0) {
			return 0;
		}
		// sum = statbuf.st_blocks * statbuf.st_blksize;
		sum = statbuf.st_size;
	}

	if (S_ISDIR(statbuf.st_mode)) {
		DIR *dir;
		struct dirent *entry;
		std::string newfile;

		dir = opendir(filename.c_str());
		if (!dir) {
			return sum;
		}

		while ((entry = readdir(dir))) {
			if (strcmp(entry->d_name, "..") == 0 || strcmp(entry->d_name, ".") == 0)
				continue;
			newfile = filename + "/" + entry->d_name;
			sum += du(newfile);
		}
		closedir(dir);
	} else {
		return sum;
	}

	return sum;
}

void InfoCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if (argv.size() < 1 || argv.size() > 3) {
        ret = "-ERR wrong number of arguments for 'info' command\r\n";
        return;
    }
    argv.pop_front();
    bool allsections = false;
    std::string section = "";
    int sections = 0;

    if (!argv.empty()) {
        section = argv.front();
        transform(section.begin(), section.end(), section.begin(), ::tolower);
        argv.pop_front();
        if (section == "all") {
          allsections = true;
        }
    } else {
        allsections = true;
    }

    std::string info;
    // Server
    if (allsections || section == "server") {
        //TODO mode : standalone cluster sentinel

        if (sections++) info.append("\r\n");

        static bool call_uname = true;
        static struct utsname name;
        if (call_uname) {
            uname(&name);
            call_uname = false;
        }
        uint64_t dbsize_B = du(g_pikaConf->db_path());
        uint64_t dbsize_M = dbsize_B / (1024 * 1024); 
        uint64_t logsize_B = du(g_pikaConf->log_path());
        uint64_t logsize_M = logsize_B / (1024 * 1024);
        
        char buf[1024];
        time_t current_time_s = std::time(NULL);
        time_t running_time_s = current_time_s-g_pikaServer->start_time_s();
        struct tm *current_time_tm_ptr = localtime(&current_time_s);
        int32_t diff_year = current_time_tm_ptr->tm_year - g_pikaServer->start_time_tm()->tm_year;
        int32_t diff_day = current_time_tm_ptr->tm_yday - g_pikaServer->start_time_tm()->tm_yday;
        int32_t running_time_d = diff_year*365 + diff_day;
        uint32_t max = 0;
        g_pikaMario->GetStatus(&max);
        std::string safety_purge;
        if (max < 10) {
          safety_purge = "none"; 
        } else {
          safety_purge = "write2file";
          char str_num[32];
          snprintf(str_num, sizeof(str_num), "%u", max-10);
          safety_purge.append(str_num);
        }
        snprintf (buf, sizeof(buf),
                  "# Server\r\n"
                  "pika_version:%s\r\n"
                  "os:%s %s %s\r\n"
                  "arch_bits:%s\r\n"
                  "process_id:%ld\r\n"
                  "tcp_port:%d\r\n"
                  "thread_num:%d\r\n"
                  "uptime_in_seconds:%d\r\n"
                  "uptime_in_days:%d\r\n"
                  "config_file:%s\r\n"
                  "is_bgsaving:%s\r\n"
                  "is_scaning_keyspace:%s\r\n"
                  "is_compact:%s\r\n"
                  "db_size:%luM\r\n"
                  "log_size:%luM\r\n"
                  "compression:%s\r\n"
                  "safety_purge:%s\r\n"
                  "expire_logs_days:%d\r\n"
                  "expire_logs_nums:%d\r\n",
                  PIKA_VERSION,
                  name.sysname, name.release, name.machine,
                  (std::string(name.machine).substr(std::string(name.machine).length()-2)).c_str(),
                  (long) getpid(),
                  g_pikaConf->port(),
                  g_pikaConf->thread_num(),
                  running_time_s,
                  running_time_d,
                  g_pikaConf->conf_path(),
                  g_pikaServer->is_bgsaving().c_str(),
                  g_pikaServer->is_scaning().c_str(),
                  g_pikaServer->GetHandle()->GetCurrentTaskType().c_str(),
                  dbsize_M,
                  logsize_M,
                  g_pikaConf->compression(),
                  safety_purge.c_str(),
                  g_pikaConf->expire_logs_days(),
                  g_pikaConf->expire_logs_nums()
                  );
        info.append(buf);
    }

    // Clients
    if (allsections || section == "clients") {
        if (sections++) info.append("\r\n");

        std::string clients;
        char buf[128];
        snprintf (buf, sizeof(buf),
                  "# Clients\r\n"
                  "connected_clients:%d\r\n",
                  (g_pikaServer->client_num_.load()));
                  //g_pikaServer->ClientList(clients));
        info.append(buf);
    }

    // Stats
    if (allsections || section == "stats") {
        if (sections++) info.append("\r\n");

        std::string clients;
        char buf[128];
        snprintf (buf, sizeof(buf),
                  "# Stats\r\n"
                  "total_connections_received:%lu\r\n"
                  "instantaneous_ops_per_sec:%d\r\n"
                  "accumulative_query_nums:%lu\r\n",
                  g_pikaServer->HistoryClientsNum(),
                  g_pikaServer->CurrentQps(),
                  g_pikaServer->CurrentAccumulativeQueryNums());
        info.append(buf);
    }

    // Replication
    if (allsections || section == "replication") {
        if (sections++) info.append("\r\n");

        int repl_state = g_pikaServer->repl_state();
        int ms_state = g_pikaServer->ms_state_;

        char buf[512];
        char ms_state_name[][20] = {
          "offline", "connect", "connecting", "connected", "single"};
        std::string ro;
        if (repl_state == PIKA_SINGLE) {
            ro = "single";
        } else {
            LOG(INFO) << "master: " << (repl_state & PIKA_MASTER);
            if ((repl_state & PIKA_MASTER) != 0) {
                ro = "master ";
            }
            LOG(INFO) << "slave: " << (repl_state & PIKA_SLAVE);
            if ((repl_state & PIKA_SLAVE) != 0) {
                ro.append("slave");
            }
        }

        if ((repl_state==PIKA_SINGLE) || !(repl_state&~PIKA_MASTER))
        {
            std::string slave_list_str;
            snprintf(buf, sizeof(buf),
                "# Replication(MASTER)\r\n"
                "role:%s\r\n"
                "connected_slaves:%d\r\n",
                ro.c_str(),
                g_pikaServer->GetSlaveList(slave_list_str));
            info.append(buf);
            info.append(slave_list_str);
        } else if (!(repl_state&~PIKA_SLAVE)) {
            std::string ms_state_str;
            switch(g_pikaServer->ms_state_)
            {
                case PIKA_REP_OFFLINE: ms_state_str = "down"; break;
                case PIKA_REP_CONNECT: ms_state_str = "down"; break;
                case PIKA_REP_CONNECTING: ms_state_str = "down"; break;
                case PIKA_REP_CONNECTED: ms_state_str = "up"; break;
                case PIKA_REP_SINGLE: ms_state_str = "down"; break;
            }
            snprintf(buf, sizeof(buf),
                "# Replication(SLAVE)\r\n"
                "role:slave\r\n"
                "master_host:%s\r\n"
                "master_port:%d\r\n"
                "master_link_status:%s\r\n"
                "slave-read-only:%d\r\n",
                (g_pikaServer->masterhost()).c_str(),
                g_pikaServer->masterport() - 100,
                ms_state_str.c_str(),
                g_pikaServer->is_readonly_);
          info.append(buf);
        } else if (!(repl_state & ~(PIKA_MASTER | PIKA_SLAVE))) {
            std::string slave_list_str;
            std::string ms_state_str;
            switch(g_pikaServer->ms_state_)
            {
                case PIKA_REP_OFFLINE: ms_state_str = "offline"; break;
                case PIKA_REP_CONNECT: ms_state_str = "connect"; break;
                case PIKA_REP_CONNECTING: ms_state_str = "connecting"; break;
                case PIKA_REP_CONNECTED: ms_state_str = "connected"; break;
                case PIKA_REP_SINGLE: ms_state_str = "single"; break;
            }
            snprintf(buf, sizeof(buf),
                "# Replication(MASTER/SLAVE)\r\n"
                "role:slave\r\n"
                "master_host:%s\r\n"
                "master_port:%d\r\n"
                "master_link_status:%s\r\n"
                "slave-read-only:%d\r\n"
                "connected_slaves:%d\r\n",
                (g_pikaServer->masterhost()).c_str(),
                g_pikaServer->masterport(),
                ms_state_str.c_str(),
                g_pikaServer->is_readonly_,
                g_pikaServer->GetSlaveList(slave_list_str));
            info.append(buf);
            info.append(slave_list_str);
        }
        /*
        snprintf (buf, sizeof(buf),
                  "# Replication\r\n"
                  "role: %s\r\n"
                  "ms_state: %s\r\n",
                  ro.c_str(),
                  ms_state_name[ms_state]);
        info.append(buf);

        if ((repl_state & PIKA_SLAVE) != 0) {
          snprintf (buf, sizeof(buf),
                  "master_host: %s\r\n"
                  "master_ip: %d\r\n",
                  g_pikaServer->masterhost().c_str(),
                  g_pikaServer->masterport());
          info.append(buf);
        }
        if ((repl_state & PIKA_MASTER) != 0) {
          std::string slave_list;
          int slave_num = g_pikaServer->GetSlaveList(slave_list);
          snprintf (buf, sizeof(buf),
                  "connected_slaves: %d\r\n",
                  slave_num);
          info.append(buf);

          if (slave_num > 0) {
            info.append(slave_list);
          }
        }
        */

    }

    // Key Space
    if (section == "keyspace") {

        char buf[512];
        std::string op = "";
        if (!argv.empty()) {
          op = argv.front();
          transform(op.begin(), op.end(), op.begin(), ::tolower);
          argv.pop_front();
          if (!argv.empty() || (op != "1" && op != "0")) {
            ret = "-ERR Invalid Argument\r\n";
            return;
          }
          if (op == "1") {
            g_pikaServer->InfoKeySpace();
          }
        }
        char infotime[32];
        strftime(infotime, sizeof(infotime), "%Y-%m-%d %H:%M:%S", localtime(&(g_pikaServer->last_info_keyspace_start_time_))); 
        snprintf (buf, sizeof(buf),
                  "# Keyspace\r\n"
                  "# Time:%s\r\n"
                  "kv keys:%lu\r\n"
                  "hash keys:%lu\r\n"
                  "list keys:%lu\r\n"
                  "zset keys:%lu\r\n"
                  "set keys:%lu\r\n",
                  infotime, g_pikaServer->last_kv_num_, g_pikaServer->last_hash_num_, g_pikaServer->last_list_num_, g_pikaServer->last_zset_num_, g_pikaServer->last_set_num_);
        info.append(buf);
    }

    ret.clear();
    char buf[32];
    snprintf (buf, sizeof(buf), "$%u\r\n", info.length());
    ret.append(buf);
    ret.append(info);
    ret.append("\r\n");
}

void PurgelogstoCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    std::string filename = argv.front();
    argv.pop_front();
    transform(filename.begin(), filename.end(), filename.begin(), ::tolower);
    if (filename.size() <= 10) {
        ret = "-ERR Invalid Argument\r\n";
        return;
    }
    std::string prefix = filename.substr(0, 10);
    if (prefix != "write2file") {
        ret = "-ERR Invalid Argument\r\n";
        return;
    }
    std::string str_num = filename.substr(10);
    int64_t num;
    if (!string2l(str_num.data(), str_num.size(), &num) || num < 0) {
        ret = "-ERR Invalid Argument\r\n";
        return;
    }
    uint32_t max = 0;
    mario::Status mas = g_pikaMario->GetStatus(&max);
    LOG(WARNING) << "max seqnum could be deleted: " << max;
    bool purge_res = false;
    if (max >= 10) {
        purge_res = g_pikaServer->PurgeLogs(max-10, num);
    }
    if (purge_res) {
        ret = "+OK\r\n";
    } else {
        ret = "-ERR write2file may in use or non_exist or already in purging...\r\n";
    }
}

void CompactCmd::Do(std::list<std::string> &argv, std::string &ret) {
    if ((arity > 0 && (int)argv.size() != arity) || (arity < 0 && (int)argv.size() < -arity)) {
        ret = "-ERR wrong number of arguments for ";
        ret.append(argv.front());
        ret.append(" command\r\n");
        return;
    }
    argv.pop_front();
    nemo::Status s = g_pikaServer->GetHandle()->Compact(nemo::kALL);
    if (s.ok()) {
      ret = "+OK\r\n";
    } else {
      ret.append("-ERR ");
      ret.append(s.ToString().c_str());
      ret.append("\r\n");
    }
}
