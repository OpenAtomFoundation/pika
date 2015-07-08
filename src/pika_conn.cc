#include "pika_conn.h"
#include "pika_util.h"
#include "pika_server.h"
#include "leveldb/db.h"
#include "pika_define.h"
#include "zmalloc.h"
#include "util.h"
#include <algorithm>
extern PikaServer *g_pikaServer;

PikaConn::PikaConn(int fd) :
    fd_(fd)
{
    thread_ = NULL;

    // init the rbuf
//    rbuf_ = (char *)malloc(sizeof(char) * PIKA_MAX_MESSAGE);
    rbuf_ = sdsempty();
    cur_pos_ = 0;
    rbuf_len_ = 0;
    req_type_ = 0;
    multibulklen_ = 0;
    bulklen_ = -1;

    wbuf_ = (char *)malloc(sizeof(char) * PIKA_MAX_MESSAGE);
    wbuf_ = sdsempty();
}

PikaConn::~PikaConn()
{
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

int PikaConn::ProcessInlineBuffer() {
    char *newline;
    int argc, j;
    sds *argv, aux;
    size_t querylen;

    /* Search for end of line */
    newline = strchr(rbuf_, '\n');

    /* Nothing to do with a \r\n */
    if (newline == NULL) {
        if (sdslen(rbuf_) > PIKA_MAX_MESSAGE) {
            log_info("Protocol error: too big inline request");
        }
        return -1;
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
        log_info("Protocol error: unbalance quotes in request");
        return -1;
    }

    /* Leave data after the first line of query in the buffer */
    sdsrange(rbuf_, querylen+2, -1);

    /* push back the argvs */
    for (j = 0; j < argc; j++) {
        if (sdslen(argv[j])) {
            argv_.push_back(std::string(argv[j], sdslen(argv[j])));
        } else {
            sdsfree(argv[j]);
        }
    }
    zfree(argv);
    return 0;
}

int PikaConn::ProcessMultibulkBuffer() {
    char *newline = NULL;
    int pos = 0, ok;
    long long ll;

    if (multibulklen_== 0) {
        /* The client should have been reset */
//        redisAssertWithInfo(c,NULL,c->argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        newline = strchr(rbuf_,'\r');
        if (newline == NULL) {
            if (sdslen(rbuf_) > PIKA_MAX_MESSAGE) {
//                addReplyError(c,"Protocol error: too big mbulk count string");
//                setProtocolError(c,0);
                  log_info("Protocol error: too big mbulk count string");
            }
            return -1;
        }

        /* Buffer should also contain \n */
        if (newline-(rbuf_) > ((signed)sdslen(rbuf_)-2))
            return -1;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
//        redisAssertWithInfo(c,NULL,c->querybuf[0] == '*');
        ok = string2ll(rbuf_+1,newline-(rbuf_+1),&ll);
        if (!ok || ll > 1024*1024) {
//            addReplyError(c,"Protocol error: invalid multibulk length");
//            setProtocolError(c,pos);
            log_info("Protocol error: invalid multibulk length");
            return -1;
        }

        pos = (newline-rbuf_)+2;
        if (ll <= 0) {
            sdsrange(rbuf_,pos,-1);
            return 0;
        }

        multibulklen_ = ll;

        /* Setup argv array on client structure */
//        if (c->argv) zfree(c->argv);
//        c->argv = zmalloc(sizeof(robj*)*c->multibulklen);
        argv_.clear();
    }

//    redisAssertWithInfo(c,NULL,c->multibulklen > 0);
    while(multibulklen_) {
        /* Read bulk length if unknown */
        if (bulklen_ == -1) {
            newline = strchr(rbuf_+pos,'\r');
            if (newline == NULL) {
                if (sdslen(rbuf_) > PIKA_MAX_MESSAGE) {
//                    addReplyError(c,
//                        "Protocol error: too big bulk count string");
//                    setProtocolError(c,0);
//                    return REDIS_ERR;
                    log_info("Protocol error: too big bulk count string");
                    return -1;
                }
                break;
            }

            /* Buffer should also contain \n */
            if (newline-(rbuf_) > ((signed)sdslen(rbuf_)-2))
                break;

            if (rbuf_[pos] != '$') {
//                addReplyErrorFormat(c,
//                    "Protocol error: expected '$', got '%c'",
//                    c->querybuf[pos]);
//                setProtocolError(c,pos);
//                return REDIS_ERR;
                log_info("Protocol error: expected '$', got %c", rbuf_[pos]);
                return -1;
            }

            ok = string2ll(rbuf_+pos+1,newline-(rbuf_+pos+1),&ll);
            if (!ok || ll < 0 || ll > 512*1024*1024) {
//                addReplyError(c,"Protocol error: invalid bulk length");
//                setProtocolError(c,pos);
//                return REDIS_ERR;
                log_info("Protocol error: invalid bulk length");
                return -1;
            }

            pos += newline-(rbuf_+pos)+2;
            if (ll >= PIKA_MAX_MESSAGE) {
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
                bulklen_ >= PIKA_MAX_MESSAGE &&
                (signed) sdslen(rbuf_) == bulklen_+2)
            {
//                c->argv[c->argc++] = createObject(REDIS_STRING,c->querybuf);
                argv_.push_back(std::string(rbuf_, bulklen_));
                sdsIncrLen(rbuf_,-2); /* remove CRLF */
                rbuf_ = sdsempty();
                /* Assume that if we saw a fat argument we'll see another one
                 * likely... */
                rbuf_ = sdsMakeRoomFor(rbuf_,bulklen_+2);
                pos = 0;
            } else {
//                c->argv[c->argc++] =
//                    createStringObject(c->querybuf+pos,c->bulklen);
                argv_.push_back(std::string(rbuf_+pos, bulklen_));
                pos += bulklen_+2;
            }
            bulklen_ = -1;
            multibulklen_--;
        }
    }

    /* Trim to pos */
    if (pos) sdsrange(rbuf_,pos,-1);

    /* We're done when c->multibulk == 0 */
    if (multibulklen_ == 0) return 0;

    /* Still not read to process the command */
    return -1;
}

int PikaConn::ProcessInputBuffer() {
    while (sdslen(rbuf_)) {
        if (!req_type_) {
            if (rbuf_[0] == '*') {
                req_type_ = REDIS_REQ_MULTIBULK;
            } else {
                req_type_ = REDIS_REQ_INLINE;
            }
        }
        if (req_type_ == REDIS_REQ_INLINE) {
            if (ProcessInlineBuffer() != 0) break;
        } else if (req_type_ == REDIS_REQ_MULTIBULK) {
            if (ProcessMultibulkBuffer() != 0) break;
        } else {
            log_info("Unknown requeset type");
            return -1;
        }
        if (GetArgc() == 0) {
            Reset();
        } else {
//            std::list<std::string>::iterator iter;
//            for (iter = argv_.begin(); iter != argv_.end(); iter++) {
//                log_info("%s", (*iter).c_str());
//            }
            if (DoCmd() == 0) {
                if(PikaSendReply() != 0) return -1;
            }
            Reset();
            return 1;
        }
    }
    return 0;
}

int PikaConn::PikaGetRequest()
{
    int nread = 0;
    int readlen = (1024*1024*64);
    size_t qblen;
    if (req_type_ == REDIS_REQ_MULTIBULK && multibulklen_ && bulklen_ != -1 
        && bulklen_ >= PIKA_MAX_MESSAGE) {
        
        int remaining = (unsigned)(bulklen_+2) - sdslen(rbuf_);
        if (remaining < readlen) readlen = remaining;
    }
    qblen = sdslen(rbuf_);
    rbuf_ = sdsMakeRoomFor(rbuf_, readlen);
    nread = read(fd_, rbuf_ + qblen, PIKA_MAX_MESSAGE);
    if (nread == -1) {
        if (errno == EAGAIN) {
            nread = 0;
        } else {
            return -1;
        }
    } else if (nread == 0) {
        log_info("client closed connection");
        return -1;
    }


    if (nread) {
        sdsIncrLen(rbuf_,nread);
    }

    return ProcessInputBuffer();
}

int PikaConn::PikaSendReply()
{
    ssize_t nwritten = 0;
    while (sdslen(wbuf_) > 0) {
        nwritten = write(fd_, wbuf_, sdslen(wbuf_));
        if (nwritten <= 0) {
            break;
        }
        sdsrange(wbuf_, nwritten, -1);
        if (nwritten == -1) {
            if (errno == EAGAIN) {
                nwritten = 0;
            } else {
                /*
                 * Here we clear this connection
                 */
                return 0;
            }
        }
    }
    if (sdslen(wbuf_) == 0) {
        return 0;
    } else {
        return -1;
    }
}

int PikaConn::DoCmd() {
    std::string opt = argv_.front();
    transform(opt.begin(), opt.end(), opt.begin(), ::tolower);
    if (opt == "set" && argv_.size() >= 3) {
        argv_.pop_front();
        std::string key = argv_.front();
        argv_.pop_front();
        std::string value = argv_.front();
        argv_.pop_front();
        leveldb::Status s = g_pikaServer->db_->Put(leveldb::WriteOptions(), key, value);
        if (s.ok()) {
            wbuf_ = sdscat(wbuf_, "+OK\r\n");        
        } else {
            std::string ret;
            ret.append("-ERR ");
            ret.append(s.ToString().c_str());
            ret.append("\r\n");
            wbuf_ = sdscat(wbuf_, ret.c_str());
        }
    } else if (opt == "get" && argv_.size() >= 2) {
        argv_.pop_front();
        std::string key = argv_.front();
        argv_.pop_front();
        std::string value;
        leveldb::Status s = g_pikaServer->db_->Get(leveldb::ReadOptions(), key, &value);
        if (s.ok()) {
            char buf[32];
            std::string ret;
            snprintf(buf, sizeof(buf), "$%d\r\n", (int)value.size());
            ret.append(buf);
            ret.append(value.data(), value.size());
            ret.append("\r\n");
            wbuf_ = sdscat(wbuf_, ret.c_str());
        } else if (s.IsNotFound()) {
            wbuf_ = sdscat(wbuf_, "$-1\r\n");
        } else {
            std::string ret;
            ret.append("-ERR ");
            ret.append(s.ToString().c_str());
            ret.append("\r\n");
            wbuf_ = sdscat(wbuf_, ret.c_str());
        }
    } else {
            std::string ret;
            ret.append("-ERR unknown or unsupported command \'");
            ret.append(opt);
            ret.append("\'\r\n");
            wbuf_ = sdscat(wbuf_, ret.c_str());        
    }
    return 0;
}

