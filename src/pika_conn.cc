#include <glog/logging.h>
#include "pika_conn.h"
#include "pika_util.h"
#include "pika_command.h"
#include "pika_define.h"
#include "pika_conf.h"
#include "pika_server.h"
#include "zmalloc.h"
#include "util.h"
#include "mario.h"
#include <algorithm>
extern std::map<std::string, Cmd *> g_pikaCmd;
extern PikaConf *g_pikaConf;
extern PikaServer *g_pikaServer;
extern mario::Mario *g_pikaMario;

PikaConn::PikaConn(int fd, std::string ip_port, int role) :
    fd_(fd), ip_port_(ip_port), role_(role)
{
    thread_ = NULL;

    // init the rbuf
    rbuf_ = sdsempty();
    cur_pos_ = 0;
    rbuf_len_ = 0;
    req_type_ = 0;
    multibulklen_ = 0;
    bulklen_ = -1;
    should_close_after_reply = false;
    wbuf_ = sdsempty();
    msbuf_ = sdsempty();
    gettimeofday(&tv_, NULL);
    is_authed_ = std::string(g_pikaConf->requirepass()) == "" ? true : false;
    querynums_ = 0;
}

PikaConn::~PikaConn()
{
//TODO?
    sdsfree(msbuf_);
    sdsfree(wbuf_);
    sdsfree(rbuf_);
}

bool PikaConn::SetNonblock()
{
    flags_ = Setnonblocking(fd_);
    if (flags_ == -1) {
        return false;
    }
    return true;
}

void PikaConn::Reset() {
    argv_.clear();
    req_type_ = 0;
    multibulklen_ = 0;
    bulklen_ = -1;
}

void PikaConn::append_wbuf_nowait(const std::string &item) {
    MutexLock l(&mutex_);
    wbuf_ = sdscatlen(wbuf_, item.data(), item.size());
}

void PikaConn::append_wbuf(const std::string &item) {
    mutex_.Lock();
    while (sdslen(wbuf_) > PIKA_SLAVE_WBUF_MAX_SIZE) {
        mutex_.Unlock();
//        LOG(INFO) << "wbuf_ is too big, wait...";
        sleep(5);
        mutex_.Lock();
    }
    wbuf_ = sdscatlen(wbuf_, item.data(), item.size());
    mutex_.Unlock();
}

int PikaConn::ProcessInlineBuffer(std::string &err_msg) {
    char *newline;
    int argc, j;
    sds *argv, aux;
    size_t querylen;
    err_msg.clear();

    /* Search for end of line */
    newline = strchr(rbuf_, '\n');

    /* Nothing to do with a \r\n */
    if (newline == NULL) {
        if (sdslen(rbuf_) > PIKA_INLINE_MAX_SIZE) {
            err_msg = "-ERR: Protocol error: too big inline request\r\n";
            sdsrange(rbuf_, 0, -1);
        }
        return -2;
    }

    /* Handle the \r\n case. */
    if (newline && newline != rbuf_ && *(newline-1) == '\r')
        newline--;

    /* Split the input buffer up to the \r\n */
    querylen = newline-rbuf_;
    aux = sdsnewlen(rbuf_, querylen);
    argv = sdssplitargs(aux, &argc);
    sdsfree(aux);
    if (argv == NULL) {
        err_msg = "-ERR: Protocol error: unbalance quotes in request\r\n";
        sdsrange(rbuf_, 0, -1);
        return -2;
    }

    msbuf_ = sdscpylen(msbuf_, rbuf_, querylen+2);
    /* Leave data after the first line of query in the buffer */
    sdsrange(rbuf_, querylen+2, -1);

    /* push back the argvs */
    for (j = 0; j < argc; j++) {
        if (sdslen(argv[j])) {
            argv_.push_back(std::string(argv[j], sdslen(argv[j])));
            sdsfree(argv[j]);
        } else {
            sdsfree(argv[j]);
        }
    }
    zfree(argv);
    return 0;
}

int PikaConn::ProcessMultibulkBuffer(std::string &err_msg) {
    char *newline = NULL;
    int pos = 0, ok;
    long long ll;
    err_msg.clear();

    if (multibulklen_== 0) {
        /* The client should have been reset */
        //TODO: redisAssertWithInfo(c,NULL,c->argc == 0);
        //Reset();

        /* Multi bulk length cannot be read without a \r\n */
        newline = strchr(rbuf_,'\r');
        if (newline == NULL) {
            if (sdslen(rbuf_) > PIKA_INLINE_MAX_SIZE) {
                  err_msg = "-ERR: Protocol error: too big mbulk count string\r\n";
                  sdsrange(rbuf_, 0, -1);
            }
            return -2;
        }

        /* Buffer should also contain \n */
        if (newline-(rbuf_) > ((signed)sdslen(rbuf_)-2))
            return -1;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
//      TODO: redisAssertWithInfo(c,NULL,c->querybuf[0] == '*');
        if (rbuf_[0] != '*') {
//            log_info("protocol exepect *,but it is %c", rbuf_[0]);
            LOG(WARNING) << "(" << ip_port_ << ")protocol expect *, but it is "<<rbuf_[0];
            return -2;
        }
        ok = string2ll(rbuf_+1,newline-(rbuf_+1),&ll);
        if (!ok || ll > 1024*1024) {
            err_msg = "-ERR: Protocol error: invalid multibulk length\r\n";
            sdsrange(rbuf_, pos, -1);
            return -2;
        }

        pos = (newline-rbuf_)+2;
        if (ll <= 0) {
            sdsrange(rbuf_, pos, -1);
            return 0;
        }

        multibulklen_ = ll;
    }
    //TODO: redisAssertWithInfo(c,NULL,c->multibulklen > 0);
    if (multibulklen_ <= 0) {
//        log_info("multi bulk len < 0 in this packet");
        LOG(WARNING) << "(" << ip_port_ << ")multi bulk len < 0 in this packet";
        return -2;
    }
    while(multibulklen_) {
        /* Read bulk length if unknown */
        if (bulklen_ == -1) {
            newline = strchr(rbuf_+pos,'\r');
            if (newline == NULL) {
                if (sdslen(rbuf_) > PIKA_INLINE_MAX_SIZE) {
                    err_msg = "-ERR: Protocol error: too big bulk count string\r\n";
                    sdsrange(rbuf_, 0, -1);
                    return -2;
                }
                break;
            }

            /* Buffer should also contain \n */
            if (newline-(rbuf_) > ((signed)sdslen(rbuf_)-2))
                break;

            if (rbuf_[pos] != '$') {
                err_msg = "-ERR: Protocol error: expected '$'\r\n";
                sdsrange(rbuf_, pos, -1);
                return -2;
            }

            ok = string2ll(rbuf_+pos+1,newline-(rbuf_+pos+1),&ll);
            if (!ok || ll < 0 || ll > 512*1024*1024) {
                err_msg = "-ERR: Protocol error: invalid bulk length\r\n";
                sdsrange(rbuf_, pos, -1);
                return -2;
            }

            pos += newline-(rbuf_+pos)+2;
            if (ll >= PIKA_MBULK_BIG_ARG) {
                size_t qblen;

                /* If we are going to read a large object from network
                 * try to make it likely that it will start at c->querybuf
                 * boundary so that we can optimize object creation
                 * avoiding a large copy of data. */
                sdsrange(rbuf_,pos,-1);
                pos = 0;
                qblen = sdslen(rbuf_);
                /* Hint the sds library about the amount of bytes this string is
                 * going to contain. */
                if (qblen < (size_t)ll+2)
                    rbuf_ = sdsMakeRoomFor(rbuf_,ll+2-qblen);
            }
            bulklen_ = ll;
        }

        /* Read bulk argument */
        if (sdslen(rbuf_)-pos < (unsigned)(bulklen_+2)) {
            /* Not enough data (+2 == trailing \r\n) */
            break;
        } else {
            /* Optimization: if the buffer contains JUST our bulk element
             * instead of creating a new object by *copying* the sds we
             * just use the current sds string. */
            if (pos == 0 &&
                bulklen_ >= PIKA_MBULK_BIG_ARG &&
                (signed) sdslen(rbuf_) == bulklen_+2)
            {
                argv_.push_back(std::string(rbuf_, bulklen_));
                sdsIncrLen(rbuf_,-2); /* remove CRLF */
                rbuf_ = sdsempty();
                /* Assume that if we saw a fat argument we'll see another one
                 * likely... */
                rbuf_ = sdsMakeRoomFor(rbuf_,bulklen_+2);
                pos = 0;
            } else {
                argv_.push_back(std::string(rbuf_+pos, bulklen_));
                pos += bulklen_+2;
            }
            bulklen_ = -1;
            multibulklen_--;
        }
    }

    /* Trim to pos */
    if (pos) {
        msbuf_ = sdscatlen(msbuf_, rbuf_, pos);
        sdsrange(rbuf_, pos, -1);
    }

    /* We're done when c->multibulk == 0 */
    if (multibulklen_ == 0) return 0;

    /* Still not read to process the command */
    return -1;
}

int PikaConn::ProcessInputBuffer() {
    std::string err_msg;
    int cmd_ret = 0;
    while (sdslen(rbuf_)) {
        if (!req_type_) {
            if (rbuf_[0] == '*') {
                req_type_ = PIKA_REQ_MULTIBULK;
            } else {
                req_type_ = PIKA_REQ_INLINE;
            }
        }
        if (req_type_ == PIKA_REQ_INLINE) {
            if (ProcessInlineBuffer(err_msg) != 0) {
                if (!err_msg.empty()) {
                    if (role_ == PIKA_SLAVE) {
                        MutexLock l(&mutex_);
                        wbuf_ = sdscat(wbuf_, err_msg.c_str());
                    } else {
                        wbuf_ = sdscat(wbuf_, err_msg.c_str());
                    }
                    sdsclear(msbuf_);
                    LOG(WARNING) << "(" << ip_port_ << ")error in parsing inlinebuffer, error: " << err_msg;
                    return -2;
                }
                break;
            }
        } else if (req_type_ == PIKA_REQ_MULTIBULK) {
            if (ProcessMultibulkBuffer(err_msg) != 0) {
                if (!err_msg.empty()) {
                    if (role_ == PIKA_SLAVE) {
                        MutexLock l(&mutex_);
                        wbuf_ = sdscat(wbuf_, err_msg.c_str());
                    } else {
                        wbuf_ = sdscat(wbuf_, err_msg.c_str());
                    }
                    sdsclear(msbuf_);
                    LOG(WARNING) << "(" << ip_port_ << ")error in parsing multibulkbuffer, error: " << err_msg;
                    return -2;
                }
                break;
            }
        } else {
//            log_info("Unknown requeset type");
            LOG(WARNING) << "(" << ip_port_ << ")Unknown requeset type";
            return -2;
        }
        if (GetArgc() == 0) {
            Reset();
        } else {
//            std::list<std::string>::iterator iter;
//            for (iter = argv_.begin(); iter != argv_.end(); iter++) {
//                log_info("%s", (*iter).c_str());
//            }
            cmd_ret = DoCmd();
            if (cmd_ret == -2) {
                should_close_after_reply = true;
                sdsclear(msbuf_);
                return -2;
            }
            if (cmd_ret == 1) {
                g_pikaMario->Put(std::string(msbuf_, sdslen(msbuf_)));
            }
            querynums_++;
            sdsclear(msbuf_);
            Reset();
        }
    }
    return 0;
}

int PikaConn::PikaGetRequest()
{
    if (should_close_after_reply) {
        return 1;
    }
    int nread = 0;
    int readlen = PIKA_IOBUF_LEN;
    size_t qblen;
    if (req_type_ == PIKA_REQ_MULTIBULK && multibulklen_ && bulklen_ != -1 
        && bulklen_ >= PIKA_MBULK_BIG_ARG) {
        
        int remaining = (unsigned)(bulklen_+2) - sdslen(rbuf_);
        if (remaining < readlen) readlen = remaining;
    }
    qblen = sdslen(rbuf_);
    rbuf_ = sdsMakeRoomFor(rbuf_, readlen);
    nread = read(fd_, rbuf_ + qblen, readlen);
    if (nread == -1) {
        if (errno == EAGAIN) {
            return 0;
        } else {
//            log_info("Reading from client: %s", strerror(errno));
            LOG(WARNING) << "(" << ip_port_ << ")Reading from client: " << strerror(errno);
            //TODO: close connection
            Reset();
            return -2;
        }
    } else if (nread == 0) {
//        log_info("client closed connection");
        LOG(INFO) << "(" << ip_port_ << ")client closed connection";
        //TODO: close connection
        Reset();
        return -2;
    }

    if (nread) {
        sdsIncrLen(rbuf_,nread);
    }
    return ProcessInputBuffer();
}

int PikaConn::PikaSendReply()
{
    ssize_t nwritten = 0;
    if (role_ == PIKA_SLAVE)  {
        mutex_.Lock();
        if (sdslen(wbuf_) == 0) {
            mutex_.Unlock();
            usleep(10000);
            return 0;
        } else {
            mutex_.Unlock();
            MutexLock l(&mutex_);
            while (sdslen(wbuf_) > 0) {
                nwritten = write(fd_, wbuf_, sdslen(wbuf_));
                if (nwritten == -1) {
                    if (errno == EAGAIN) {
                        nwritten = 0;
                    } else {
                        /*
                         * Here we clear this connection
                         */
                        LOG(WARNING) << "(" << ip_port_ << ")send error, close client";
                        should_close_after_reply = true;
                        return 0;
                    }
                }
                if (nwritten <= 0) {
                    break;
                }
                sdsrange(wbuf_, nwritten, -1);
            }
            if (sdslen(wbuf_) == 0) {
                return 0;
            } else {
                return -1;
            }
        }
    } else {
        while (sdslen(wbuf_) > 0) {
 //           LOG(INFO) << "start write";
            nwritten = write(fd_, wbuf_, sdslen(wbuf_));
            if (nwritten == -1) {
                if (errno == EAGAIN) {
                    nwritten = 0;
                } else {
                    /*
                     * Here we clear this connection
                     */
                    LOG(WARNING) << "(" << ip_port_ << ")send error, close client";
                    should_close_after_reply = true;
                    return 0;
                }
            }
            if (nwritten <= 0) {
                break;
            }
            sdsrange(wbuf_, nwritten, -1);
        }
        if (sdslen(wbuf_) == 0) {
//            LOG(INFO) << "write success";
            return 0;
        } else {
//            LOG(INFO) << "write fail";
            return -1;
        }
    }
}

int PikaConn::DoCmd() {
    std::string opt = argv_.front();
    transform(opt.begin(), opt.end(), opt.begin(), ::tolower);
    int cmd_ret = 0;
    std::string ret;
    if (is_authed_ || opt == "auth" || opt == "slaveauth") {
        std::map<std::string, Cmd *>::iterator iter = g_pikaCmd.find(opt);
        if (iter == g_pikaCmd.end()) {
            ret.append("-ERR unknown or unsupported command \'");
            ret.append(opt);
            ret.append("\'\r\n");
        } else {
            if (opt == "pikasync") {
                char buf[32];
                ll2string(buf, sizeof(buf), fd_);
                argv_.push_back(std::string(buf));
                mutex_.Lock();
            }
            pthread_rwlock_rdlock(g_pikaServer->rwlock());

            if (g_pikaServer->is_readonly_ == true && iter->second->is_sync == true && role_ != PIKA_MASTER) {
                pthread_rwlock_unlock(g_pikaServer->rwlock());
                ret = "-ERR Server in Readonly\r\n";
            } else {
                iter->second->Do(argv_, ret);
                if (opt != "loaddb" && opt != "dump" && opt != "readonly" && opt != "ucanpsync" && opt != "flushall") {
                    pthread_rwlock_unlock(g_pikaServer->rwlock());
                }
                if (opt == "auth") {
                    if (ret == "+OK\r\n") {
                        is_authed_ = true;
                    } else {
                        is_authed_ = false;
                        LOG(WARNING) << "(" << ip_port_ << ")Wrong Password, close connection";
                        cmd_ret = -2;
                    }

                    if (std::string(g_pikaConf->requirepass()) == "") {
                        ret = "-ERR Client sent AUTH, but no password is set\r\n";
                    }
                }
                if (opt == "slaveauth") {
                    if (ret == "+OK\r\n") {
                        is_authed_ = true;
                    } else {
                        LOG(WARNING) << "(" << ip_port_ << ")Slave Wrong Password, close connection";
                        is_authed_ = false;
                        cmd_ret = -2;
                    }
                    ret = "";

                    if (std::string(g_pikaConf->requirepass()) == "") {
                        ret = "-ERR Client sent AUTH, but no password is set\r\n";
                    }
                }
                if (iter->second->is_sync == true && ret.find("-ERR ") != 0) {
                    cmd_ret = 1;
                }
            }
        }
    } else {
        ret = "-ERR NOAUTH Authentication required.\r\n";
        LOG(WARNING) << "(" << ip_port_ << ")Authentication required, close connection";
        cmd_ret = -2;
    }
    if (opt == "pikasync") {
        wbuf_ = sdscatlen(wbuf_, ret.data(), ret.size());
        mutex_.Unlock();
    } else if (role_ != PIKA_MASTER && !(role_ == PIKA_SLAVE && opt == "ping")) {
        if (role_ == PIKA_SLAVE) {
            MutexLock l(&mutex_);
            wbuf_ = sdscatlen(wbuf_, ret.data(), ret.size());
        } else {
            wbuf_ = sdscatlen(wbuf_, ret.data(), ret.size());
        }
    }
    return cmd_ret;
}

