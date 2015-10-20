#include "pika_conn.h"
#include "mario.h"

class MarioHandler : public mario::Consumer::Handler
{
public:
    MarioHandler(std::string &ip, int port, PikaConn* conn) : ip_(ip), port_(port), conn_(conn) {};

    ~MarioHandler() {}

    virtual bool processMsg(const std::string &item) {
        conn_->append_wbuf(item);
        return true;
    }
    std::string ip_;
    int port_;
    PikaConn* conn_;
};
