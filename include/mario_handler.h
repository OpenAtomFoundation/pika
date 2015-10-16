#include "mario.h"
#include <glog/logging.h>
#include <netinet/tcp.h>

class MarioHandler : public mario::Consumer::Handler
{
public:
    MarioHandler(std::string ip, int port) : 
    ip_(ip),
    port_(port){
        wbuf_ = sdsempty();
        rbuf_ = sdsempty();
    };

    ~MarioHandler() {
//        close(sockfd_);
        delete rbuf_;
        delete wbuf_;
    }

    int pika_connect() {
        sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd_ == -1) {
            LOG(WARNING) << "socket error " << strerror(errno);
            return -1;
        }
        int flags = fcntl(sockfd_, F_GETFL, 0);
        fcntl(sockfd_, F_SETFL, flags | O_NONBLOCK);
        int yes = 1;
        if (setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
            LOG(WARNING) << "setsockopt SO_REUSEADDR: " << strerror(errno);
            return -1;
        }
        if (setsockopt(sockfd_, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) == -1) {
            LOG(WARNING) << "setsockopt SO_KEEPALIVE: " << strerror(errno);
            return -1;
        }
//        if (setsockopt(sockfd_, SOL_SOCKET, TCP_NODELAY, &yes, sizeof(yes)) == -1) {
//            LOG(WARNING) << "setsockopt TCP_NODELAY: " << strerror(errno);
//            return -1;
//        }

        struct sockaddr_in s_addr;
        memset(&s_addr, 0, sizeof(s_addr));
        s_addr.sin_family = AF_INET;
        s_addr.sin_addr.s_addr = inet_addr(ip_.c_str());
        s_addr.sin_port = htons(port_);
        
        int retry = 10;
        int ret;
        while ((ret = connect(sockfd_, (struct sockaddr*)(&s_addr), sizeof(s_addr))) == -1 && errno == EINPROGRESS && retry) {
            retry--;
        }
        if (ret == -1) {
            LOG(INFO) << "failed to connect to slave";
            return -1;
        }
        std::string bm = "*1\r\n$8\r\nbemaster\r\n";
        wbuf_ = sdscpylen(wbuf_, bm.data(), bm.size());
        ssize_t nwritten = 0;
        while (sdslen(wbuf_) > 0) {
            nwritten = write(sockfd_, wbuf_, sdslen(wbuf_));
            if (nwritten == -1) {
                if (errno == EAGAIN) {
                    nwritten = 0;
                } else {
                    /*
                     * Here we clear this connection
                     */

                    LOG(INFO) << "failed to write bemaster command to slave";
                    close(sockfd_);
                    return -1;
                }
            }
            sdsrange(wbuf_, nwritten, -1);
        }
        if (sdslen(wbuf_) == 0) {
            return 0;
        } else {
            LOG(INFO) << "failed to write bemaster command to slave";
            close(sockfd_);
            return -1;
        }
    }

    int reconnect() {
        close(sockfd_);
        return pika_connect();
    }

    virtual bool processMsg(const std::string &item) {
        wbuf_ = sdscpylen(wbuf_, item.data(), item.size());
        ssize_t nwritten = 0;
        while (sdslen(wbuf_) > 0) {
            nwritten = write(sockfd_, wbuf_, sdslen(wbuf_));
            if (nwritten == -1) {
                if (errno == EAGAIN) {
                    nwritten = 0;
                } else {
                    /*
                     * Here we clear this connection
                     */

                    LOG(INFO) << "failed to write command to slave";
                    reconnect();
                    return false;
                }
            }
            sdsrange(wbuf_, nwritten, -1);
        }
        if (sdslen(wbuf_) == 0) {
            return true;
        } else {
            LOG(INFO) << "failed to write command to slave";
            reconnect();
            return false;
        }
    }

    std::string ip_;
    int port_;
    int sockfd_;
    sds wbuf_;
    sds rbuf_;
};
