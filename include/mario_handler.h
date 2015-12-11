#include "pika_conn.h"
#include "mario.h"

class MarioHandler : public mario::Consumer::Handler
{
public:
    MarioHandler(PikaConn* conn) : conn_(conn) {};

    ~MarioHandler() {}

    virtual bool processMsg(const std::string &item) {
        conn_->append_wbuf(item);
        return true;
    }
    PikaConn* conn_;
};
